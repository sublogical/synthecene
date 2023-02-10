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
    proc: Box<dyn FnOnce(T) -> BehaviorState<T,E> + Send + 'static>,
}

impl <T:Send, E> Behavior<T, E> 
where
    T: Send + 'static,
    E: Debug + Send + 'static
{
    fn new<F>(proc: F) -> BehaviorState<T, E> 
    where F: FnOnce(T) -> BehaviorState<T, E> + Send +'static
    {
        Ok(Behavior {
            proc: Box::new(proc),
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

    fn unrecoverable(error: E) -> BehaviorState<T, E>
    {
        Err(BehaviorMode::UnrecoverableError(error))
    }
}

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
        let guardian = ActorRef::from_behavior(guardian);
        ActorSystem {
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


struct Actor<T,E>
where
    T: Send + 'static,
    E: Debug + Send + 'static
{
    receiver: mpsc::Receiver<T>,
    state: BehaviorState<T, E>
}

impl <T, E> Actor<T,E>
where
    T: Send + 'static,
    E: Debug + Send + 'static
{
    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            if self.state.is_err() {
                panic!("Actor behavior is not set");
            }
            let prior_behavior = replace(&mut self.state, Err(BehaviorMode::NoBehavior)).unwrap();
            match (prior_behavior.proc)(msg) {
                Ok(behavior) => self.state = Ok(behavior),
                Err(mode) => {
                    todo!("Handle mode: {:?}", mode);
                    self.state = Err(mode)
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ActorRef<T> {
    sender: mpsc::Sender<T>,
}

impl <T> ActorRef<T> 
where
    T: Send + 'static,
{
    pub fn from_behavior<E>(state: BehaviorState<T, E>) -> Self 
    where
        E: Debug + Send + 'static
    {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Actor {
            receiver,
            state
        };

        tokio::spawn(async move {
            actor.run().await;
        });

        Self { sender }
    }

    pub fn oneshot() -> (mpsc::Receiver<T>, Self) {
        let (sender, receiver) = mpsc::channel(8);
        (
            receiver,
            Self {
                sender
            }
        )
    }
    async fn run_actor<E>(mut actor: Actor<T, E>)
    where
        E: Debug + Send + 'static
    {
        actor.run().await;
    }

    pub async fn tell(&self, msg: T) {
        let _ = self.sender.send(msg).await;
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
        let _ = self.sender.send(msg).await;
        receiver.recv().await.ok_or_else(|| {
            todo!("Handle error")
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
            Behavior::new(move |(text, respond_to):(String, ActorRef<usize>)| {
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
            Behavior::new(move |(text, respond_to):(String, ActorRef<usize>)| {
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

    /*
    async fn test_actor_lifecycle() {
        struct SpawnJob {};
        struct StartJob {};

        let record = Arc::new(RwLock::new(vec![]));
        
        fn job(num: usize, record: Arc<RwLock<Vec<String>>>) -> BehaviorState<StartJob, ()> {
            Behavior::new(move |_| {
                record.write().unwrap().push(format!("job {}", num));
                if num == 0 {
                    Behavior::stopped()
                } else {
                    job(num - 1, record)
                }
            })
            .with_signal(move |signal| {
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
     */
}
