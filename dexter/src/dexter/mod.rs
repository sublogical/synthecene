use std::{mem::replace, fmt, sync::{Arc, RwLock}};

use derivative::Derivative;
use futures::FutureExt;
use log::{debug, info, error};
use tokio::{sync::{mpsc}, task::JoinHandle};
use std::fmt::Debug;

pub mod settable;

/// Represents the planned behavior of an actor. The caller can define the behavior of a running actor by
/// implementing a Running or RunningWatcher behavior. The Running behavior defines how the actor should
/// respond to a message, and will be called zero or once. The Running behaviors proc is responsible for 
/// returning a new behavior for the actor. Any necessary state is expected to be owned by the behavior
/// proc closure.
/// 
/// 
/// The proc can also return one of the terminal behavioars, which will cause the actor to stop. This includes:
/// - Stopped: The actor will stop and not be restarted.
/// - RecoverableError: The actor will stop and can be restarted.
/// - UnrecoverableError: The actor will stop and cannot be restarted.
///  
/// Note that the system itself will not restart actors. If this is desired, a supervisor actor should be used.
 
enum Behavior<I, O, E> 
where
    I: Debug + Send + 'static,
    O: Debug + Send + 'static,
    E: Debug + Send + 'static,
{
    NoBehavior,
    Running {
        proc: Box<dyn FnOnce(&mut ActorContext<I,O,E>, I)->Behavior<I, O, E> + Send + Sync + 'static>
    },

    /// RunningWatcher extends Running by allowing the behavior to also provide a signal handler.
    /// Signal handlers are also expected to propogate any state changes to the next behavior.
    RunningWatcher {
        proc: Box<dyn FnOnce(&mut ActorContext<I,O,E>, I)->Behavior<I,O,E> + Send + Sync + 'static>,
        signal_proc: Box<dyn FnOnce(&mut ActorContext<I,O,E>, Signal)->Behavior<I,O,E> + Send + Sync + 'static>
    },

    /// RecoverableError is a terminal behavior that will cause the actor to stop. It can be restarted.
    RecoverableError(E),

    /// UnrecoverableError is a terminal behavior that will cause the actor to stop. It can be restarted.
    UnrecoverableError(E),
    Stopped(O),
}

impl <T, O, E> fmt::Display for Behavior<T, O, E> 
where
    T: Debug + Send + 'static,
    O: Debug + Send + 'static,
    E: Debug + Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let sz = match self {
            Behavior::NoBehavior => "no behavior",
            Behavior::Running { proc:_ } => "running",
            Behavior::RunningWatcher { proc:_, signal_proc:_ } => "running-watcher",
            Behavior::RecoverableError(_) => "error: recoverable",
            Behavior::UnrecoverableError(_) => "error: unrecoverable",
            Behavior::Stopped(_) => "stopped",
        };

        write!(f, "{}", sz)
    }
}

impl <I, O, E> Behavior<I, O, E> 
where
    I: Debug + Send + 'static,
    O: Debug + Send + 'static,
    E: Debug + Send + 'static
{
    fn running(proc: impl FnOnce(&mut ActorContext<I,O,E>, I)->Behavior<I,O,E> + Send + Sync + 'static) -> Self {
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
    
    fn stopped(output:O) -> Self {
        Self::Stopped(output)
    }
    
    fn with_signal(self, signal_proc: impl FnOnce(&mut ActorContext<I,O,E>, Signal)->Behavior<I,O,E> + Send + Sync + 'static) -> Self {
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
enum SystemState<O, E> {
    Running,
    RecoverableError(E),
    UnrecoverableError(E),
    Stopped(O)
}

impl <I, O, E> From<Behavior<I, O, E>> for SystemState<O, E> 
where
    I: Debug + Send + 'static,
    O: Debug + Send + 'static,
    E: Debug + Send + 'static
{
    fn from(behavior: Behavior<I, O, E>) -> Self {
        match behavior {
            Behavior::Stopped(o) => SystemState::Stopped(o),
            Behavior::RecoverableError(e) => SystemState::RecoverableError(e),
            Behavior::UnrecoverableError(e) => SystemState::UnrecoverableError(e),
            _ => SystemState::Running
        }
    }
}

impl <O, E> From<SystemState<O, E>> for Result<O, E> 
where
    O: Debug + Send + 'static,
    E: Debug + Send + 'static
{
    fn from(state: SystemState<O, E>) -> Self {
        match state {
            SystemState::Stopped(o) => Ok(o),
            SystemState::RecoverableError(e) => Err(e),
            SystemState::UnrecoverableError(e) => Err(e),
            _ => panic!("cannot convert running system state to result")
        }
    }
}

impl <O, E> From<&SystemState<O, E>> for Signal
where
    O: Debug + Send + 'static,
    E: Debug + Send + 'static
{
    fn from(state: &SystemState<O, E>) -> Self {
        match state {
            SystemState::Stopped(_) => Signal::Stopped,
            SystemState::RecoverableError(_) | SystemState::UnrecoverableError(_) => Signal::Stopped,
            _ => Signal::Started
        }
    }
}

impl <O, E> SystemState<O, E> {
    fn is_running(&self) -> bool {
        match self {
            SystemState::Running => true,
            _ => false
        }
    }

    fn is_stopped(&self) -> bool {
        !self.is_running()
    }
}

struct ActorSystem<I, O, E>
where
    I: Debug + Send + 'static,
    O: Debug + Send + 'static,
    E: Debug + Send + 'static
{
    root_context: ActorContext<I, O, E>,
    guardian: ActorRef<I>,
    completion: JoinHandle<Result<O, E>>,
}

impl <I, O, E> ActorSystem<I, O, E> 
where
    I: Debug + Send + 'static,
    O: Debug + Send + Sync + 'static,
    E: Debug + Send + Sync + 'static
{
    fn new(guardian: Behavior<I, O, E>, name: &str) -> ActorSystem<I, O, E> 
    {
        info!("ActorSystem starting with guardian: {}", name);

        let mut root_context = ActorContext::root();        
        let (guardian, completion) = root_context.spawn(guardian, name);

        ActorSystem {
            root_context,
            guardian,
            completion,
        }
    }
    pub async fn tell(&self, msg: I) {
        let _ = self.guardian.tell(msg).await;
    }

    pub async fn ask<R, F>(&self, init: F) -> Result<R , E>
    where 
        F: FnOnce(ActorRef<R>) -> I,
        R: Send + 'static,
    {
        self.guardian.ask(init).await
    }

    pub fn is_stopped(&self) -> bool {
        self.completion.is_finished()
    }

    pub async fn wait_for_stop(self) -> Result<O, E> {
        self.completion.await.expect("actor system completion failed")
    }

}

#[derive(Debug)]
enum Envelope<T> {
    Signal(Signal),
    Message(T)
}

struct ActorContext<I, O, E>
where
    I: Debug + Send + 'static,
    O: Debug + Send + 'static,
    E: Debug + Send + 'static
{
    receiver: mpsc::Receiver<Envelope<I>>,
    sender: mpsc::Sender<Envelope<I>>,

    state: Behavior<I, O, E>
}

impl <I, O, E> ActorContext<I, O, E>
where
    I: Debug + Send + 'static,
    O: Debug + Send + 'static,
    E: Debug + Send + 'static
{
    pub fn spawn<II, OO, EE> (&mut self, 
        state: Behavior<II, OO, EE>, 
        name: &str) -> (ActorRef<II>, JoinHandle<Result<OO, EE>>)
    where
        II: Debug + Send + 'static,
        OO: Debug + Send + 'static,
        EE: Debug + Send + 'static
    {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = ActorContext {
            receiver,
            sender: sender.clone(),
            state
        };

        let parent_sender = self.sender.clone();

        let result = tokio::spawn(async move {
            // todo: send ChildStarted signal to myself (must use self.sender)
            let child_state:SystemState<OO, EE> = actor.run().await.into();

            info!("child terminated with state: {:?}", child_state);

            // todo: find a better syntax for this. Should be able to "into" without taking ownership
            let signal = (&child_state).into();
            let _ = parent_sender.send(Envelope::Signal(signal)).await;

            child_state.into()
        });

        (ActorRef { sender }, result)
    }

    // todo: send started and post-stopped signals to the the actor

    async fn run(mut self) -> Behavior<I, O, E> {
        while self.state.is_runnable() {
            if let Some(env) = self.receiver.recv().await {
                let prior_behavior = replace(&mut self.state, Behavior::NoBehavior);
                self.state = match env {
                    Envelope::Signal(signal) => match prior_behavior {
                        Behavior::RunningWatcher { signal_proc, .. } => signal_proc(&mut self, signal),
                        _ => {
                            info!("Actor received signal: {:?} but no signal proc was defined", signal);
                            prior_behavior
                        }
                    },
                    Envelope::Message(msg) => match prior_behavior {
                        Behavior::Running { proc } | Behavior::RunningWatcher { proc, .. } => proc(&mut self, msg),
                        _ => {
                            error!("Actor received message: {:?} but no message proc was defined", msg);
                            prior_behavior
                        }
                    }
                };
            }
        }
        info!("Actor stopped with state: {}", self.state);
        self.state
    }

    fn self_ref(&self) -> ActorRef<I> {
        ActorRef {
            sender: self.sender.clone()
        }
    }

    fn root() -> ActorContext<I, O, E> {
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

    #[ctor::ctor]
    fn init() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug).is_test(true).try_init();
    }

    #[tokio::test]
    async fn test_simple_actor() {
        fn basic() -> Behavior<(String, ActorRef<usize>), (), ()> {
            Behavior::running(move |_, (text, respond_to):(String, ActorRef<usize>)| {
                debug!("Received message: {:?}", text);
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
        fn count(num:usize) -> Behavior<(String, ActorRef<usize>), (), ()> {
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

    #[tokio::test]
    async fn test_actor_lifecycle() {
        #[derive(Debug)]
        struct SpawnJob {};

        #[derive(Debug)]
        struct StartJob {};

        let record = Arc::new(RwLock::new(vec![]));
        
        fn job(num: usize, record: Arc<RwLock<Vec<String>>>) -> Behavior<StartJob, (), ()> {
            let signal_record = record.clone();

            Behavior::running(move |context, _| {
                debug!("job {} running", num);
                record.write().unwrap().push(format!("job {}", num));
                if num == 0 {
                    debug!("job {} stopping", num);
                    Behavior::Stopped(())
                } else {
                    let me = context.self_ref();
                    tokio::spawn(async move { me.tell(StartJob{}).await });
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
                        job(num, signal_record)
                    },
                    Signal::Stopped => {
                        signal_record.write().unwrap().push(format!("job stopped"));
                        Behavior::Stopped(())
                    }
                }
            })
        }
    
        fn controller(record: Arc<RwLock<Vec<String>>>) -> Behavior<SpawnJob, (), ()> {
            let signal_record = record.clone();

            Behavior::running(move |actor_context, start:SpawnJob| {
                record.write().unwrap().push(format!("start job"));
                let (job, _) = actor_context.spawn(job(10, record.clone()), "job");
                tokio::spawn(async move { job.tell(StartJob {}).await; });
                controller(record)
            })
            .with_signal(move |_, signal| {
                match signal {
                    Signal::ChildTerminated  => {
                        debug!("child stopping");
                        signal_record.write().unwrap().push(format!("child terminated"));
                        Behavior::Stopped(())
                    },
                    Signal::Started => {
                        signal_record.write().unwrap().push(format!("controller started"));
                        controller(signal_record)
                    },
                    Signal::Stopped => {
                        signal_record.write().unwrap().push(format!("controller stopped"));
                        Behavior::Stopped(())
                    }
                }
            })
        }

        let system = ActorSystem::new(controller(record.clone()), "test");
        system.tell(SpawnJob {}).await;

        let result = system.wait_for_stop().await;
        assert!(result.is_ok());

        // todo: wait for job to finish
        // todo: validate that the lifecycle events were recorded in correct order
    }

    #[tokio::test]
    async fn test_wait_for_stop_ok() {
        fn stop() -> Behavior<String, u64, ()> {
            Behavior::running(move |_, _:String| {
                // todo: add ability to stop & return value
                Behavior::Stopped(42)
            })
        }

        let system = ActorSystem::new(stop(), "test");
        system.tell("hi".to_string()).await;

        let result = system.wait_for_stop().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_wait_for_stop_no_ok() {
        // todo: add a failure case
    }

    #[tokio::test]
    async fn test_stop_guardian() {
        // todo: add a test that stops the guardian
    }

    #[tokio::test]
    async fn test_stop_guardian_with_children() {
        // todo: add a test that stops the guardian with children
    }

    #[tokio::test]
    async fn test_stop_child() {
        // todo: add a test that stops a child
    }


}
