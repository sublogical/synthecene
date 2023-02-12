use std::{mem::replace, fmt, sync::{Arc, RwLock}};

use derivative::Derivative;
use tokio::sync::{mpsc};
use std::fmt::Debug;

pub mod settable;

enum Behavior<T, E> 
where
    T: Send + 'static,
    E: Debug + Send + 'static,
{
    NoBehavior,
    Running {
        proc: Box<dyn FnOnce(&mut ActorContext<T,E>, T)->Behavior<T,E> + Send + Sync + 'static>
    },
    RunningWatcher {
        proc: Box<dyn FnOnce(&mut ActorContext<T,E>, T)->Behavior<T,E> + Send + Sync + 'static>,
        signal_proc: Box<dyn FnOnce(&mut ActorContext<T,E>, Signal)->Behavior<T,E> + Send + Sync + 'static>
    },
    UnrunnableWatcher {
        signal_proc: Box<dyn FnOnce(&mut ActorContext<T,E>, Signal)->Behavior<T,E> + Send + Sync + 'static>
    },
    RecoverableError(E),
    UnrecoverableError(E),
    Stopped,
}

impl <T, E> fmt::Display for Behavior<T, E> 
where
    T: Send + 'static,
    E: Debug + Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let sz = match self {
            Behavior::NoBehavior => "no behavior",
            Behavior::Running { proc:_ } => "running",
            Behavior::RunningWatcher { proc:_, signal_proc:_ } => "running-watcher",
            Behavior::UnrunnableWatcher { signal_proc:_ } => "unrunnable-watcher",
            Behavior::RecoverableError(_) => "error: recoverable",
            Behavior::UnrecoverableError(_) => "error: unrecoverable",
            Behavior::Stopped => "stopped",
        };

        write!(f, "{}", sz)
    }
}

impl <T, E> Behavior<T, E> 
where
    T: Send + 'static,
    E: Debug + Send + 'static
{
    fn running(proc: impl FnOnce(&mut ActorContext<T,E>, T)->Behavior<T,E> + Send + Sync + 'static) -> Self {
        Self::Running {
            proc: Box::new(proc)
        }
    }

    fn recoverable(e:E) -> Self {
        Self::RecoverableError(e)
    }

    fn unrecoverable(e:E) -> Self {
        Self::UnrecoverableError(e)
    }
    
    fn stopped() -> Self {
        Self::Stopped
    }
    
    fn with_signal(self, signal_proc: impl FnOnce(&mut ActorContext<T,E>, Signal)->Behavior<T,E> + Send + Sync + 'static) -> Self {
        match self {
            Self::Running{ proc } => Self::RunningWatcher { proc, signal_proc: Box::new(signal_proc) },
            _ => self
        }
    }

    fn is_runnable(&self) -> bool {
        match self {
            Self::Running { .. } | Self::RunningWatcher { .. } => true,
            _ => false
        }
    }
}

#[derive(Debug)]
enum Signal {
    Started,
    Stopped,
    ChildTerminated
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum SystemState {
    Running,
    Stopped
}

struct ActorSystem<T, E>
where
    T: Send + 'static,
    E: Debug + Send + 'static
{
    root_context: ActorContext<T, E>,
    guardian: ActorRef<T>,
    state: Arc<RwLock<SystemState>>,
    phantom: std::marker::PhantomData<E>,
}

impl <T, E> ActorSystem<T, E> 
where
    T: Debug + Send + 'static,
    E: Debug + Send + 'static
{
    fn new(guardian: Behavior<T, E>, name: &str) -> ActorSystem<T, E> 
    {
        let state = Arc::new(RwLock::new(SystemState::Running));

        let signal_state = state.clone();
        let mut root_context = ActorContext::root();        
        let guardian = root_context.spawn_with_term_proc(guardian, name, move |state| {
            println!("guardian terminated: {}", state);
            let mut state = signal_state.write().unwrap();
            *state = SystemState::Stopped;
        });

        ActorSystem {
            root_context,
            guardian,
            state,
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
        *(self.state.read().unwrap()) == SystemState::Stopped
    }
}

#[derive(Debug)]
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

    state: Behavior<T, E>
}

impl <T, E> ActorContext<T,E>
where
    T: Debug + Send + 'static,
    E: Debug + Send + 'static
{
    pub fn spawn<TT, EE> (&mut self, state: Behavior<TT, EE>, name: &str) -> ActorRef<TT>
    where
        TT: Debug + Send + 'static,
        EE: Debug + Send + 'static
    {
        self.spawn_with_term_proc(state, name, |state| {
            println!("child terminated: {}", state);
        })
    }

    pub fn spawn_with_term_proc<TT, EE> (&mut self, 
        state: Behavior<TT, EE>, 
        name: &str, 
        term_proc: impl FnOnce(Behavior<TT,EE>)-> () + Send + 'static) -> ActorRef<TT>
    where
        TT: Debug + Send + 'static,
        EE: Debug + Send + 'static
    {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = ActorContext {
            receiver,
            sender: sender.clone(),
            state
        };

        let parent_sender = self.sender.clone();

        tokio::spawn(async move {
            // todo: send ChildStarted signal to myself (must use self.sender)
            let child_state = actor.run().await;

            // todo: figure out how to encode the child's error type in the signal, anyhow?
            let signal = Signal::ChildTerminated;

            println!("root context signal received: {:?}", signal);
            // Send the ChildTerminated signal to the parent
            let res = parent_sender.send(Envelope::Signal(signal)).await;
            println!("sent tp parent: {:?}", res);

            term_proc(child_state);
        });

        ActorRef { sender }
    }

    // todo: send started and post-stopped signals to the the actor

    async fn run(mut self) -> Behavior<T, E> {
        while self.state.is_runnable() {
            if let Some(env) = self.receiver.recv().await {
                let prior_behavior = replace(&mut self.state, Behavior::NoBehavior);
                self.state = match env {
                    Envelope::Signal(signal) => match prior_behavior {
                        // todo: route to behavior's signal proc
                        Behavior::RunningWatcher { signal_proc, .. } => signal_proc(&mut self, signal),
                        _ => {
                            println!("Actor received signal: {:?} but no signal proc was defined", signal);
                            prior_behavior
                        }
                    },
                    Envelope::Message(msg) => match prior_behavior {
                        Behavior::Running { proc } | Behavior::RunningWatcher { proc, .. } => proc(&mut self, msg),
                        _ => {
                            println!("Actor received message: {:?} but no message proc was defined", msg);
                            prior_behavior
                        }
                    }
                };
            }
        }
        println!("actor stopped with state: {}", self.state);
        self.state
    }

    fn root() -> ActorContext<T, E> {
        let (sender, receiver) = mpsc::channel(8);
        ActorContext {
            receiver,
            sender,
            state: Behavior::NoBehavior
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

        fn basic() -> Behavior<(String, ActorRef<usize>), ()> {
            Behavior::running(move |_, (text, respond_to):(String, ActorRef<usize>)| {
                println!("Received message: {:?}", text);
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
        fn count(num:usize) -> Behavior<(String, ActorRef<usize>), ()> {
            Behavior::running(move |_, (text, respond_to):(String, ActorRef<usize>)| {
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
        #[derive(Debug)]
        struct SpawnJob {};

        #[derive(Debug)]
        struct StartJob {};

        let record = Arc::new(RwLock::new(vec![]));
        
        fn job(num: usize, record: Arc<RwLock<Vec<String>>>) -> Behavior<StartJob, ()> {
            let signal_record = record.clone();

            Behavior::running(move |_, _| {
                record.write().unwrap().push(format!("job {}", num));
                if num == 0 {
                    Behavior::Stopped
                } else {
                    job(num - 1, record)
                }
            })
            .with_signal(move |_, signal| {
                match signal {
                    Signal::ChildTerminated  => {
                        panic!{"should not receive child terminated, doesn't create children"}
                    },
                    Signal::Started => {
                        signal_record.write().unwrap().push(format!("job started"));
                    },
                    Signal::Stopped => {
                        signal_record.write().unwrap().push(format!("job stopped"));
                    }
                }
                job(num, signal_record)
            })
        }
    
        fn controller(record: Arc<RwLock<Vec<String>>>) -> Behavior<SpawnJob, ()> {
            let signal_record = record.clone();

            Behavior::running(move |actor_context, start:SpawnJob| {
                record.write().unwrap().push(format!("start job"));
                let job = actor_context.spawn(job(10, record.clone()), "job");
                tokio::spawn(async move { job.tell(StartJob {}).await; });
                controller(record)
            })
            .with_signal(move |_, signal| {
                match signal {
                    Signal::ChildTerminated  => {
                        signal_record.write().unwrap().push(format!("child terminated"));
                    },
                    Signal::Started => {
                        signal_record.write().unwrap().push(format!("controller started"));
                    },
                    Signal::Stopped => {
                        signal_record.write().unwrap().push(format!("controller stopped"));
                    }
                }
                controller(signal_record)
            })
        }

        let system = ActorSystem::new(controller(record.clone()), "test");
        system.tell(SpawnJob {}).await;

        // todo: wait for job to finish
        // todo: validate that the lifecycle events were recorded in correct order
    }
}
