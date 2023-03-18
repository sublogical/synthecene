use std::{mem::replace, fmt, sync::{Arc, RwLock}};

use derivative::Derivative;
use futures::FutureExt;
use log::{debug, info, error};
use tokio::{sync::{mpsc}, task::JoinHandle};
use std::fmt::Debug;

pub mod settable;

#[derive(Debug)]
pub enum Error
{
    ErrorReceivingMessage,
    ErrorSendingMessage,
    UnexpectedSignal,
}


/***
 * WHAT ISN"T WORKING
 * 
 * - need some form of default "signal" handling, enabling cleanup of child actors
 * - need to give the actor a way to handle shutdown requests gracefully. Maybe? 
 * - alternately, we could just say shutdown is shutdown. the system automatically 
 * - propogates the shutdown throught actor context
 * 
 * 
 * Behavior.new - build a behavior from scratch
 * Behavior.prior - build a behavior using any remaining prior procs
 * Behavior.stopped(O) - the actor has stopped, and will not be restarted, no signals will be sent to the actor
 * Behavior.recoverable_error(E) - the actor has stopped, and can be restarted, no signals will be sent to the actor
 * Behavior.unrecoverable_error(E) - the actor has stopped, and will not be restarted, no signals will be sent to the actor
 * 
 * builder.add_signal_handler(signal, |ctx, signal| {
 *    Behavior::stopped(())
 * })
 *
 * ```rust
 * Behavior.new(|ctx, msg| {
 *   match msg {
 *     ...
 *   }
 * }).add_signal_handler(Signal::ChildTerminated, |ctx, signal| {
 *    Behavior::stopped(())
 * })
 * ```
 */


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
        signal_procs: Map<
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
    ChildTerminated(String),
    RequestStop,
    ForceStop
}

impl Signal {
    fn default_proc <I, O, E>(&self, context: ActorContext<I, O, E>)
    where
        I: Debug + Send + 'static,
        O: Debug + Send + 'static,
        E: Debug + Send + 'static
    {
        match self {
            Signal::Started => {
                debug!("${}: actor started", context.name);
            },
            Signal::ChildTerminated(name) => {
                debug!("${}: child terminated: {:?}", context.name, name);
            },
            Signal::RequestStop => {
                info!("actor requested to stop");
            },
            Signal::ForceStop => {
                info!("actor forced to stop");
            }
        }
    }

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
    I: Debug + Send + Clone + 'static,
    O: Debug + Send + 'static,
    E: Debug + Send + 'static
{
    root_context: ActorContext<I, O, E>,
    guardian: ActorRef<I>,
    completion: JoinHandle<Result<O, E>>,
}

impl <I, O, E> ActorSystem<I, O, E> 
where
    I: Debug + Send + Clone + 'static,
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
    pub async fn tell(&self, msg: I) -> Result<(), Error> {
        Ok(self.guardian.tell(msg).await?)
    }

    pub async fn ask<R, F>(&self, init: F) -> Result<Result<R , E>, Error>
    where 
        F: FnOnce(ActorRef<R>) -> I,
        R: Clone + Send + 'static,
    {
        self.guardian.ask(init).await
    }

    pub async fn stop(&self) -> Result<(), Error> {
        self.guardian.stop().await
    }
    pub fn is_stopped(&self) -> bool {
        self.completion.is_finished()
    }

    pub fn guardian(&self) -> ActorRef<I> {
        self.guardian.clone()
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
    state: Behavior<I, O, E>,
    name: String
}

impl <I, O, E> ActorContext<I, O, E>
where
    I: Debug + Send + Clone + 'static,
    O: Debug + Send + 'static,
    E: Debug + Send + 'static
{
    pub fn spawn<II, OO, EE> (&mut self, 
        state: Behavior<II, OO, EE>, 
        name: &str) -> (ActorRef<II>, JoinHandle<Result<OO, EE>>)
    where
        II: Debug + Send + Clone + 'static,
        OO: Debug + Send + 'static,
        EE: Debug + Send + 'static
    {
        let (sender, receiver) = mpsc::channel(8);
        let name = format!("{}:{}", self.name, name);
        let mut actor = ActorContext {
            receiver,
            sender: sender.clone(),
            state,
            name
        };

        let parent_sender = self.sender.clone();

        let result = tokio::spawn(async move {
            let child_name = actor.name.clone();
            let child_state = actor.run().await;

            // todo: find a way to encode the child's state in the signal
            let signal = Signal::ChildTerminated(child_name);
            let _ = parent_sender.send(Envelope::Signal(signal)).await;

            child_state.into()
        });

        (ActorRef { sender }, result)
    }

    // todo: send started and post-stopped signals to the the actor

    async fn run(mut self) -> SystemState<O, E> {
        self.sender.send(Envelope::Signal(Signal::Started)).await.expect("failed to send started signal");

        while self.state.is_runnable() {
            if let Some(env) = self.receiver.recv().await {
                let prior_behavior = replace(&mut self.state, Behavior::NoBehavior);
                self.state = match env {
                    Envelope::Signal(signal) => match prior_behavior {
                        Behavior::RunningWatcher { signal_proc, .. } => {
                            debug!("${}: received signal: {:?}", self.name, signal);
                            signal_proc(&mut self, signal)
                        },
                        _ => {
                            debug!("${}: received signal: {:?} but no signal proc was defined", self.name, signal);
                            prior_behavior
                        }
                    },
                    Envelope::Message(msg) => match prior_behavior {
                        Behavior::Running { proc } => proc(&mut self, msg),
                        Behavior::RunningWatcher { proc, signal_proc } => {
                            debug!("${}: received message: {:?}", self.name, msg);
                            proc(&mut self, msg)
                        },
                        _ => {
                            error!("${}: received message: {:?} but no message proc was defined", self.name, msg);
                            prior_behavior
                        }
                    }
                };

            }
        }
        let state:SystemState<O, E> = self.state.into();

        info!("${}: stopped with state: {:?}", self.name, state);
        state
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
            state: Behavior::NoBehavior,
            name: "root".to_string()
        }
    }
}

#[derive(Clone, Debug)]
pub struct ActorRef<T>
where
    T: Send + Clone + 'static,
{
    sender: mpsc::Sender<Envelope<T>>,
}

impl <T> ActorRef<T> 
where
    T: Send + Clone + 'static,
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

    pub async fn tell(&self, msg: T) -> Result<(), Error> {
        self.sender.send(Envelope::Message(msg)).await
            .map_err(|err| Error::ErrorSendingMessage)
    }

    pub async fn ask<R, E, F>(&self, init: F) -> Result<Result<R, E>, Error>
    where
        F: FnOnce(ActorRef<R>) -> T,
        R: Send + Clone + 'static,
        E: Debug + Send + 'static
    {
        let (mut receiver, response_actor) = ActorRef::oneshot();
        let msg = init(response_actor);

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check for the
        // same failure twice.
        let _ = self.sender.send(Envelope::Message(msg)).await;
        let response = receiver.recv().await;


        let output:Result<Result<R, E>, Error> = response.map_or_else(|| {
                Err(Error::ErrorReceivingMessage)
            }, |env| match env {
                Envelope::Message(msg) => Ok(Ok(msg)),
                Envelope::Signal(_) => {
                    let e = Error::UnexpectedSignal;
                    Err(e)
                }
            });
        output
    }

    pub async fn stop(&self) -> Result<(), Error> {
        self.sender.send(Envelope::Signal(Signal::RequestStop)).await
            .map_err(|err| Error::ErrorSendingMessage)
    }
}

#[cfg(test)]
mod test {
    use std::{sync::{Arc, RwLock}, time::Duration};

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
        let value = system.ask(|respond_to| ("hello".to_string(), respond_to)).await.unwrap().unwrap();
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
        let value = system.ask(|respond_to| ("hello".to_string(), respond_to)).await.unwrap().unwrap();
        assert_eq!(value, 15);
    }

    #[tokio::test]
    async fn test_actor_lifecycle() {
        #[derive(Clone, Debug)]
        struct SpawnJob;

        #[derive(Clone, Debug)]
        struct StartJob;

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
            .with_signal(move |actor_context, signal| {
                match signal {
                    Signal::Started => {
                        debug!("${}: worker started", actor_context.name);
                        signal_record.write().unwrap().push(format!("job started"));
                        job(num, signal_record)
                    },
                    _  => {
                        panic!{"unexpected signal"}
                    },
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
            .with_signal(move |actor_context, signal| {
                match signal {
                    Signal::ChildTerminated(child)  => {
                        debug!("${}: child terminated: {}", actor_context.name, child);
                        signal_record.write().unwrap().push(format!("child terminated"));
                        Behavior::Stopped(())
                    },
                    Signal::Started => {
                        debug!("${}: controller started", actor_context.name);
                        signal_record.write().unwrap().push(format!("controller started"));

                        let me = actor_context.self_ref();
                        tokio::spawn(async move { me.tell(SpawnJob {}).await; });

                        controller(signal_record)
                    },
                    _ => signal.default_proc(context)
                }
            })
        }

        let system = ActorSystem::new(controller(record.clone()), "test");
        let result = system.wait_for_stop().await;
        assert!(result.is_ok());

        let expected:Vec<String> = vec![
            "controller started",
            "start job",
            "job started",
            "job 10",
            "job 9",
            "job 8",
            "job 7",
            "job 6",
            "job 5",
            "job 4",
            "job 3",
            "job 2",
            "job 1",
            "job 0",
            "child terminated",
        ].iter().map(|&s|s.into()).collect();

        assert_eq!(record.read().unwrap().clone(), expected);
        // todo: wait for job to finish
        // todo: validate that the lifecycle events were recorded in correct order
    }

    #[tokio::test]
    async fn test_wait_for_stop_ok() {
        fn stop() -> Behavior<String, u64, ()> {
            Behavior::running(move |_, _:String| {
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
        #[derive(Debug, PartialEq)]
        enum FreakOut {
            Panic(String)
        }
        fn stop() -> Behavior<String, u64, FreakOut> {
            Behavior::running(move |_, _:String| {
                Behavior::UnrecoverableError(FreakOut::Panic("oh no!".to_string()))
            })
        }

        let system = ActorSystem::new(stop(), "test");
        system.tell("hi".to_string()).await;

        let result = system.wait_for_stop().await;
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), FreakOut::Panic("oh no!".to_string()));
    }

    #[tokio::test]
    async fn test_stop_guardian() {
        fn immortal() -> Behavior<String, (), ()> {
            Behavior::running(move |_, _:String| {
                immortal()
            })
        }

        let system = ActorSystem::new(immortal(), "immortal");
        let me = system.guardian();

        tokio::spawn(async move { 
            tokio::time::sleep(Duration::from_millis(50)).await;
            me.stop().await;
        });

        let result = system.wait_for_stop().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ());
    }

    #[tokio::test]
    async fn test_stop_guardian_with_children() {
        // todo: add a test that stops the guardian with children
    }

    #[tokio::test]
    async fn test_stop_child() {
        // todo: add a test that stops a child
    }


    async fn test_stop_handler() {
        #[derive(Clone, Debug, PartialEq)]
        enum Work {
            DoWork(String),
            SavedWork(u64)
        }

        fn stop_handler(state:Vec<String>) -> Behavior<Work, u64, ()> {
            Behavior::running(move |_, work| {
                match work {
                    Work::DoWork(s) => {
                        state.push(s);
                        stop_handler(state)
                    },
                    Work::SavedWork(count) => {
                        panic!("should not get here");
                    }
                }
            })
            .with_signal_handler(Signal::Stopped, |context, _| {
                let me = context.self_ref();
                tokio::spawn(async move { 
                    // simulate doing something important
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    me.tell
                });
                wait_for_done()
            })
        }

        fn wait_for_done() -> Behavior<Work, u64, ()> {
            Behavior::running(move |_, work| {
                match work {
                    Work::DoWork(_) => {
                        panic!("should not get here");
                    },
                    Work::SavedWork(count) => {
                        Behavior::Stopped(count)
                    }
                }
            })
        }

        let system = ActorSystem::new(immortal(), "immortal");
        let me = system.guardian();

        me.tell(Work::DoWork("hello".to_string())).await;
        me.tell(Work::DoWork("hello".to_string())).await;
        me.tell(Work::DoWork("hello".to_string())).await;
        me.tell(Work::DoWork("hello".to_string())).await;

    }
}
