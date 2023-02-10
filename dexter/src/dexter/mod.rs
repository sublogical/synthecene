use std::{ marker::PhantomData, mem::replace, sync::RwLock};

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use std::future::Future;

#[derive(Debug)]
enum Error {
    NoBehavior,
}

struct Behavior<T, F>
where F: FnOnce(T) -> Behavior<T, F> + Send + 'static
{
    proc: Box<dyn FnOnce(T) -> Behavior<T, F>>,
}

impl <T,F> Behavior<T,F> 
where F: FnOnce(T) -> Behavior<T, F> + Send + 'static
{
    fn new(proc: F) -> Behavior<T, F> 
    {
        Behavior {
            proc: Box::new(proc),
        }
    }
}

enum SettableMsg<T> {
    Set(T),
    Get(ActorRef<T>),
}

pub struct SettableNode;

impl SettableNode {
    fn apply<O, F>() -> Behavior<SettableMsg<O>,F> 
    where
        O: Clone + Send + 'static,
        F: FnOnce(SettableMsg<O>) -> Behavior<SettableMsg<O>, F> + Send + 'static,
    {
         Self::empty(vec![])
    }

    fn empty<O, F>(mut pending: Vec<ActorRef<O>>) -> Behavior<SettableMsg<O>,F> 
    where
        O: Clone + Send + 'static,
        F: FnOnce(SettableMsg<O>) -> Behavior<SettableMsg<O>, F> + Send + 'static,
    {
        Behavior::new(move |msg:SettableMsg<O>| {
            match msg {
                SettableMsg::Set(value) => {
                    Self::notify_pending(&pending, &value);
                    Self::resolved(value)
                }
                SettableMsg::Get(respond_to) => {
                    pending.push(respond_to);

                    Self::empty(pending)
                }
            }
        })
    }

    fn notify_pending<T: Clone + Send>(pending: &Vec<ActorRef<T>>, value: &T) {
        for respond_to in pending {
            respond_to.tell(value.clone());
        }
        
    }

    fn resolved<O, F>(value: O) -> Behavior<SettableMsg<O>, F> 
    where
        O: Clone + Send + 'static,
        F: FnOnce(SettableMsg<O>) -> Behavior<SettableMsg<O>, F> + Send + 'static,
    {
        Behavior::new(move |msg:SettableMsg<O>| {
            match msg {
                SettableMsg::Set(new_value) => {
                    Self::resolved(new_value)
                }
                SettableMsg::Get(respond_to) => {
                    respond_to.tell(value.clone());
                    Self::resolved(value)
                }
            }
        })
    }
}

struct ActorSystem<T> {
    guardian: ActorRef<T>,
}

impl <T:Send> ActorSystem<T> {
    fn new<F>(guardian: Behavior<T,F>, name: &str) -> ActorSystem<T> 
    where
        F: FnOnce(T) -> Behavior<T, F> + Send + 'static,
    {
        let guardian = ActorRef::from_behavior(guardian);
        ActorSystem {
            guardian,
        }
    }
    pub async fn tell(&mut self, msg: T) {
        let _ = self.guardian.tell(msg).await;
    }

    pub async fn ask<R, F>(&self, init: F) -> R 
    where F: FnOnce(ActorRef<R>) -> T
    {
        self.guardian.ask(init).await
    }
}


struct Actor<T, F>
where
    F: FnOnce(T) -> Behavior<T, F> + Send + 'static
{
    receiver: mpsc::Receiver<T>,
    behavior: Result<Behavior<T, F>, Error>
}

impl <T, F> Actor<T, F>
where
    F: FnOnce(T) -> Behavior<T, F> + Send + 'static
{
    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            if self.behavior.is_err() {
                panic!("Actor behavior is not set");
            }
            let prior_behavior = replace(&mut self.behavior, Err(Error::NoBehavior)).unwrap();

            self.behavior = Ok((prior_behavior.proc)(msg));
        }
    }
}

#[derive(Clone)]
pub struct ActorRef<T> {
    sender: mpsc::Sender<T>,
}

impl <T: Send> ActorRef <T>  {
    pub fn from_behavior<F>(behavior: Behavior<T, F>) -> Self 
    where
        F: FnOnce(T) -> Behavior<T, F> + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Actor {
            receiver,
            behavior: Ok(behavior)
        };

        tokio::spawn(actor.run());

        Self { sender }
    }

    pub fn oneshot(sender: mpsc::Sender<T>) -> Self {
        Self {
            sender
        }
    }
    async fn run_actor<F>(mut actor: Actor<T, F>) 
    where
        F: FnOnce(T) -> Behavior<T, F> + Send + 'static,
    {
        actor.run().await;
    }

    pub async fn tell(&self, msg: T) {
        let _ = self.sender.send(msg).await;
    }

    pub async fn ask<R, F>(&self, init: F) -> R 
    where F: FnOnce(ActorRef<R>) -> T
    {
        let (sender, mut receiver) = mpsc::channel(8);

        let response_actor = ActorRef { sender };

        let msg = init(response_actor);

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check for the
        // same failure twice.
        let _ = self.sender.send(msg).await;
        receiver.recv().await.expect("Actor task has been killed")
    }
}

use futures::poll;
use std::task::Poll;

#[tokio::test]
async fn test_settable_node() {
    let system = ActorSystem::new(
        SettableNode::apply::<i32>(), "settable_node");

    let output = Box::pin(system.ask(|actor_ref| {SettableMsg::Get(actor_ref) }));
    let current = poll!(output);
    assert_eq!(current, Poll::Pending);

    let set_request = system.tell(SettableMsg::Set(42)).await;

    let current = poll!(output);
    assert_eq!(current, Poll::Ready(42));
}