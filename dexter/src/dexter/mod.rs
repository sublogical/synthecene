use std::mem::replace;

use tokio::sync::mpsc;
use std::fmt::Debug;

pub mod settable;

#[derive(Debug)]
enum Error {
    NoBehavior,
}

pub struct Behavior<T> 
where
    T: Send + 'static
{
    proc: Box<dyn FnOnce(T) -> Behavior<T> + Send>,
}

impl <T:Send> Behavior<T> {
    fn new<F>(proc: F) -> Behavior<T> 
    where F: FnOnce(T) -> Behavior<T> + Send +'static
    {
        Behavior {
            proc: Box::new(proc),
        }
    }
}

struct ActorSystem<T> {
    guardian: ActorRef<T>,
}

impl <T:Send> ActorSystem<T> {
    fn new(guardian: Behavior<T>, name: &str) -> ActorSystem<T> {
        let guardian = ActorRef::from_behavior(guardian);
        ActorSystem {
            guardian,
        }
    }
    pub async fn tell(&self, msg: T) {
        let _ = self.guardian.tell(msg).await;
    }

    pub async fn ask<R, F>(&self, init: F) -> R 
    where 
        F: FnOnce(ActorRef<R>) -> T,
        R: Send + 'static
    {
        self.guardian.ask(init).await
    }
}


struct Actor<T:Send + 'static> {
    receiver: mpsc::Receiver<T>,
    behavior: Result<Behavior<T>, Error>
}

impl <T:Send> Actor<T> {
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

#[derive(Clone, Debug)]
pub struct ActorRef<T> {
    sender: mpsc::Sender<T>,
}

impl <T: Send> ActorRef <T>  {
    pub fn from_behavior(behavior: Behavior<T>) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Actor {
            receiver,
            behavior: Ok(behavior)
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
    async fn run_actor(mut actor: Actor<T>) {
        actor.run().await;
    }

    pub async fn tell(&self, msg: T) {
        let _ = self.sender.send(msg).await;
    }

    pub async fn ask<R, F>(&self, init: F) -> R 
    where
        F: FnOnce(ActorRef<R>) -> T,
        R: Send + 'static
    {
        let (mut receiver, response_actor) = ActorRef::oneshot();
        let msg = init(response_actor);

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check for the
        // same failure twice.
        let _ = self.sender.send(msg).await;
        receiver.recv().await.expect("Actor task has been killed")
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_simple_actor() {

        fn basic() -> Behavior<(String, ActorRef<usize>)> {
            Behavior::new(move |(text, respond_to):(String, ActorRef<usize>)| {
                tokio::spawn(async move {respond_to.tell(text.len()).await});
                basic()
            })
        }

        let system = ActorSystem::new(basic(), "test");
        let value = system.ask(|respond_to| ("hello".to_string(), respond_to)).await;
        assert_eq!(value, 5);
    }

    #[tokio::test]
    async fn test_stateful_actor() {
        fn count(num:usize) -> Behavior<(String, ActorRef<usize>)> {
            Behavior::new(move |(text, respond_to):(String, ActorRef<usize>)| {
                let new_num = text.len() + num;
                tokio::spawn(async move {respond_to.tell(new_num).await});
                count(new_num)
            })
        }

        let system = ActorSystem::new(count(0), "test");
        system.ask(|respond_to| ("hello".to_string(), respond_to)).await;
        system.ask(|respond_to| ("hello".to_string(), respond_to)).await;
        let value = system.ask(|respond_to| ("hello".to_string(), respond_to)).await;
        assert_eq!(value, 15);

    }
}
