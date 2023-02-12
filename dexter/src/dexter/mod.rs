use std::mem::replace;

use tokio::sync::mpsc;
use std::fmt::Debug;

pub mod settable;

#[derive(Debug)]
pub enum BehaviorMode<E> {
    NoBehavior,
    RecoverableError(E),
    UnrecoverableError(E),
    Stopped,
}

pub type BehaviorState<T, E> = std::result::Result<Behavior<T, E>, BehaviorMode<E>>;

pub struct Behavior<T, E> 
where
    T: Send + 'static,
    E: Debug + Send + 'static
{
    proc: Box<dyn FnOnce(&mut ActorContext<T,E>, T) -> BehaviorState<T,E> + Send + 'static>,
    signal_proc: Option<Box<dyn FnOnce(&mut ActorContext<T,E>,Signal) -> BehaviorState<T,E> + Send + 'static>>,
}

impl <T:Send, E> Behavior<T, E> 
where
    T: Send + 'static,
    E: Debug + Send + 'static
{
    fn new<F>(proc: F) -> BehaviorState<T, E> 
    where F: FnOnce(&mut ActorContext<T,E>,T) -> BehaviorState<T, E> + Send +'static
    {
        Ok(Behavior {
            proc: Box::new(proc),
            signal_proc: None
        })
    }

    fn stopped() -> BehaviorState<T, E>
    {
        Err(BehaviorMode::Stopped)
    }

    fn recoverable(error: E) -> BehaviorState<T, E>
    {
        Err(BehaviorMode::RecoverableError(error))
    }

    /**
     * Create a behavior that will never recover from an error.
     */
    fn unrecoverable(error: E) -> BehaviorState<T, E>
    {
        Err(BehaviorMode::UnrecoverableError(error))
    }

    
    /**
     * Add a signal handler to the behavior.
     */
    fn with_signal<F>(self: Result<Self, E>, signal_proc: F) -> BehaviorState<T, E>
    where
        F: FnOnce(&mut ActorContext<T,E>,Signal) -> BehaviorState<T, E> + Send + 'static
    {
        Ok(Behavior {
            proc: self.proc,
            signal_proc: Some(Box::new(signal_proc))
        })
    }
}

#[derive(Debug)]
enum Signal {
    Started,
    Stopped,
    ChildTerminated
}

struct ActorSystem<T, E>
where
    T: Send + 'static,
    E: Debug + Send + 'static
{
    root_context: ActorContext<T, E>,
    guardian: ActorRef<T>,
    phantom: std::marker::PhantomData<E>,
}

impl <T, E> ActorSystem<T, E> 
where
    T: Send + 'static,
    E: Debug + Send + 'static
{
    fn new(guardian: BehaviorState<T, E>, name: &str) -> ActorSystem<T, E> 
    {
        let mut root_context = ActorContext::root();
        let guardian = root_context.spawn(guardian);

        ActorSystem {
            root_context,
            guardian,
            phantom: std::marker::PhantomData,
        }
    }
    pub async fn tell(&self, msg: T) {
        let _ = self.guardian.tell(msg).await;
    }

    pub async fn ask<R, F>(&self, init: F) -> Result<R , E>
    where 
        F: FnOnce(ActorRef<R>) -> T,
        R: Send + 'static,
    {
        self.guardian.ask(init).await
    }

    pub fn is_stopped(&self) -> bool {
        todo!("Implement is_stopped()");
    }
}

enum Envelope<T> {
    Signal(Signal),
    Message(T)
}

struct ActorContext<T,E>
where
    T: Send + 'static,
    E: Debug + Send + 'static
{
    receiver: mpsc::Receiver<Envelope<T>>,
    sender: mpsc::Sender<Envelope<T>>,
    state: BehaviorState<T, E>
}

impl <T, E> ActorContext<T,E>
where
    T: Send + 'static,
    E: Debug + Send + 'static
{
    pub fn spawn(&mut self, state: BehaviorState<T, E>) -> ActorRef<T>
    where
        E: Debug + Send + 'static
    {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = ActorContext {
            receiver,
            sender: sender.clone(),
            state
        };

        tokio::spawn(async move {
            // send ChildTerminated signal to myself
            actor.run().await;
            // send ChildTerminated signal to myself
        });

        ActorRef { sender }
    }


    // todo: send started and post-stopped signals to the the actor

    async fn run(&mut self) {
        while !self.state.is_err() {
            if let Some(env) = self.receiver.recv().await {
                if self.state.is_err() {
                    // since there is no way for the state to be mutated 
                    // outside of this loop, this shouldn't be a state. 
                    // Best to panic and fix the bug.
                    panic!("Actor is in error state in recv loop");
                }
                let prior_behavior = replace(&mut self.state, Err(BehaviorMode::NoBehavior)).unwrap();
                match env {
                    Envelope::Signal(signal) => {
                        // todo: route to behavior's signal proc
                        match prior_behavior.signal_proc {
                            Some(signal_proc) => {
                                self.state = signal_proc(self, signal)
                                    .map_err(|mode| {
                                        todo!("Handle mode: {:?}", mode);
                                        mode
                                    })
                            },
                            None => {
                                println!("Actor received signal: {:?} but no signal proc was defined", signal);
                            }
                        }
                    },
                    Envelope::Message(msg) => {
                        self.state = (prior_behavior.proc)(self, msg)
                            .map_err(|mode| {
                                todo!("Handle mode: {:?}", mode);
                                mode
                            })
                    }
                }
            }
        }
    }

    fn root() -> ActorContext<T, E> {
        let (sender, receiver) = mpsc::channel(8);
        ActorContext {
            receiver,
            sender,
            state: Err(BehaviorMode::NoBehavior)
        }
    }
}

#[derive(Clone, Debug)]
pub struct ActorRef<T> {
    sender: mpsc::Sender<Envelope<T>>,
}

impl <T> ActorRef<T> 
where
    T: Send + 'static,
{
    fn oneshot() -> (mpsc::Receiver<Envelope<T>>, Self) {
        let (sender, receiver) = mpsc::channel(8);
        (
            receiver,
            Self {
                sender
            }
        )
    }

    pub async fn tell(&self, msg: T) {
        let _ = self.sender.send(Envelope::Message(msg)).await;
    }

    pub async fn ask<R, E, F>(&self, init: F) -> Result<R, E>
    where
        F: FnOnce(ActorRef<R>) -> T,
        R: Send + 'static,
        E: Debug + Send + 'static
    {
        let (mut receiver, response_actor) = ActorRef::oneshot();
        let msg = init(response_actor);

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check for the
        // same failure twice.
        let _ = self.sender.send(Envelope::Message(msg)).await;
        receiver.recv().await
            .map_or_else(|| {
                todo!("Handle error while waiting for response")
            }, |env| match env {
                Envelope::Message(msg) => Ok(msg),
                Envelope::Signal(_) => todo!("handle unexpected signal")
            })
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, RwLock};

    use super::*;

    #[tokio::test]
    async fn test_simple_actor() {

        fn basic() -> BehaviorState<(String, ActorRef<usize>), ()> {
            Behavior::new(move |_, (text, respond_to):(String, ActorRef<usize>)| {
                tokio::spawn(async move {respond_to.tell(text.len()).await});
                basic()
            })
        }

        let system = ActorSystem::new(basic(), "test");
        let value = system.ask(|respond_to| ("hello".to_string(), respond_to)).await.unwrap();
        assert_eq!(value, 5);
    }

    #[tokio::test]
    async fn test_stateful_actor() {
        fn count(num:usize) -> BehaviorState<(String, ActorRef<usize>), ()> {
            Behavior::new(move |_, (text, respond_to):(String, ActorRef<usize>)| {
                let new_num = text.len() + num;
                tokio::spawn(async move {respond_to.tell(new_num).await});
                count(new_num)
            })
        }

        let system = ActorSystem::new(count(0), "test");
        system.ask(|respond_to| ("hello".to_string(), respond_to)).await;
        system.ask(|respond_to| ("hello".to_string(), respond_to)).await;
        let value = system.ask(|respond_to| ("hello".to_string(), respond_to)).await.unwrap();
        assert_eq!(value, 15);
    }

    async fn test_actor_lifecycle() {
        struct SpawnJob {};
        struct StartJob {};

        let record = Arc::new(RwLock::new(vec![]));
        
        fn job(num: usize, record: Arc<RwLock<Vec<String>>>) -> BehaviorState<StartJob, ()> {
            Behavior::new(move |_, _| {
                record.write().unwrap().push(format!("job {}", num));
                if num == 0 {
                    Behavior::stopped()
                } else {
                    job(num - 1, record)
                }
            })
            .with_signal(move |_, signal| {
                match signal {
                    Signal::Started => {
                        record.write().unwrap().push(format!("job started"));
                        println!("Started");
                    },
                    Signal::PostStopped => {
                        record.write().unwrap().push(format!("job stopped"));
                        println!("Stopped");
                    }
                }
            })
        }
    
        fn controller(record: Arc<RwLock<Vec<String>>>) -> BehaviorState<SpawnJob, ()> {
            Behavior::with_context(|actor_context| {
                Behavior::new(move |start:SpawnJob| {
                    record.write().unwrap().push(format!("start job"));
                    let job = actor_context.spawn(job(10, record.clone()), "job");
                    tokio::spawn(async move { job.tell(()).await; });
                    controller(record)
                })
                .with_signal(move |signal| {
                    match signal {
                        Signal::ChildTerminated { name, reason } => {
                            record.write().unwrap().push(format!("child {} terminated: {:?}", name, reason));
                        },
                        Signal::Started => {
                            record.write().unwrap().push(format!("controller started"));
                            println!("Started");
                        },
                        Signal::PostStopped => {
                            record.write().unwrap().push(format!("controller stopped"));
                            println!("Stopped");
                        }
                    }
                })
    
            })
        }

        let system = ActorSystem::new(controller(record.clone()), "test");
        system.ask(|respond_to| ("hello".to_string(), respond_to)).await;
        system.ask(|respond_to| ("hello".to_string(), respond_to)).await;
        let value = system.ask(|respond_to| ("hello".to_string(), respond_to)).await;
        assert_eq!(value, 15);
    }
}
