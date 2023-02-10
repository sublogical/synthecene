use std::{ marker::PhantomData, mem::replace};

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
enum Error {
    NoBehavior,
}

trait Message {
    type Response;
}

struct MsgRef<T: Message> {
    content: T,
    respond_to: ActorHandle<T::Response>,
}

struct Behavior<T: Message> {
    proc: Box<dyn FnOnce(MsgRef<T>) -> Behavior<T>>,
}

impl <T: Message> Behavior<T> {
    fn new<F>(proc: F) -> Behavior<T> 
    where F: FnOnce(MsgRef<T>) -> Behavior<T> + 'static
    {
        Behavior {
            proc: Box::new(proc),
        }
    }
}

enum SettableMsg<T> {
    Set(T),
    Get,
}

impl <T> Message for SettableMsg<T> 
where T:
{
    type Response = SettableResponse<T>;
}


// todo: find a way to have a generic response type conditioned on the message type not the enum
enum SettableResponse<T> {
    Value(T),
    Ok,
}

impl <T> Message for SettableResponse<T> 
{
    type Response = ();
}


pub struct SettableNode;

impl SettableNode {
    fn apply<T: Clone + 'static>() -> Behavior<SettableMsg<T>> { Self::empty(vec![]) }

    fn empty<T: Clone + 'static>(mut pending: Vec<MsgRef<SettableMsg<T>>>) -> Behavior<SettableMsg<T>> {
        Behavior::new(move |msg| {
            match msg.content {
                SettableMsg::Set(value) => {
                    Self::notify_pending(&pending, &value);
                    msg.sender.send(SettableResponse::Ok);
                    Self::resolved(value)
                }
                SettableMsg::Get => {
                    pending.push(msg);

                    Self::empty(pending)
                }
            }
        })
    }

    fn notify_pending<T: Clone>(pending: &Vec<MsgRef<SettableMsg<T>>>, value: &T) {
        for msg in pending {
            msg.sender.send(SettableResponse::Value(value.clone()));
        }
        
    }

    fn resolved<T: Clone + 'static>(value: T) -> Behavior<SettableMsg<T>> {
        Behavior::new(move |msg| {
            match msg.content {
                SettableMsg::Set(new_value) => {
                    msg.sender.send(SettableResponse::Value(value));
                    Self::resolved(new_value)
                }
                SettableMsg::Get => {
                    msg.sender.send(SettableResponse::Value(value.clone()));
                    Self::resolved(value)
                }
            }
        })
    }
}

struct ActorSystem<T: Message> {
    guardian: Behavior<T>,
}

impl <T: Message> ActorSystem<T> {
    fn new(guardian: Behavior<T>, name: &str) -> ActorSystem<T> {
        ActorSystem {
            guardian,
        }
    }

    async fn send(&self, msg: T) -> MsgRef<T> {
        todo!()
    }
}


struct Actor<T: Message> {
    receiver: mpsc::Receiver<MsgRef<T>>,
    behavior: Result<Behavior<T>, Error>
}

impl <T: Message> Actor<T> {
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
    sender: mpsc::Sender<MsgRef<T>>,
}

impl <T: Message> ActorHandle <T>  {
    pub fn new(behavior: Behavior<T>) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let actor = Actor {
            receiver,
            behavior: Ok(behavior)
        };

        tokio::spawn(Self::run_actor(actor));

        Self { sender }
    }

    async fn run_actor(actor: Actor<T>) {
        actor.run().await;
    }

    pub async fn send(&self, msg: T) -> u32 {
        let (send, recv) = oneshot::channel();

        let msg_ref = MsgRef {
            respond_to: send,
            content: msg,
        };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check for the
        // same failure twice.
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

#[tokio::test]
async fn test_settable_node() {
    let system = ActorSystem::new(
        SettableNode::apply::<i32>(), "settable_node");

    let get_response = system.send(SettableMsg::Get);
    assert!(get_response.poll().is_pending());

    let set_request = system.send(SettableMsg::Set(42)).await.unwrap();
    assert!(set_request == SettableResponse::Ok);

    let get_response = get_response.await.unwrap();
    assert!(get_response == SettableResponse::Value(42));
}