use super::{ActorRef, Behavior};
use std::fmt::Debug;

#[derive(Debug)]
enum SettableMsg<T> {
    Set(T),
    Get(ActorRef<T>),
}

pub struct SettableNode;

impl SettableNode {
    fn apply<T>() -> Behavior<SettableMsg<T>> 
    where 
        T: Clone + Debug + Send + Sync + 'static 
    {
        Self::empty(vec![])
    }

    fn empty<T: Clone + Debug + Send + Sync + 'static>(mut pending: Vec<ActorRef<T>>) -> Behavior<SettableMsg<T>> {
        Behavior::new(move |msg:SettableMsg<T>| {
            println!("SettableNode::empty: {:?}", msg);
            match msg {
                SettableMsg::Set(value) => {
                    {
                        let value = value.clone();
                        tokio::task::spawn(async move {
                            println!("SettableNode::empty: notify_pending");
                            Self::notify_pending(&pending, &value).await;
                        });
                    }
                    Self::resolved(value)
                }
                SettableMsg::Get(respond_to) => {
                    pending.push(respond_to);

                    Self::empty(pending)
                }
            }
        })
    }

    async fn notify_pending<T: Clone + Send>(pending: &Vec<ActorRef<T>>, value: &T) {
        for respond_to in pending {
            respond_to.tell(value.clone()).await;
        }
        
    }

    fn resolved<T: Clone + Send + 'static>(value: T) -> Behavior<SettableMsg<T>> {
        Behavior::new(move |msg:SettableMsg<T>| {
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

#[cfg(test)]
mod test {
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use crate::dexter::*;
    use crate::dexter::settable::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_settable_node() {
        let system = Arc::new(ActorSystem::new(
            SettableNode::apply::<i32>(), "settable_node"));

        let output = Arc::new(RwLock::new(None));
        {
            let output = output.clone();
            let system = system.clone();

            tokio::spawn(async move {
                let get_result = system.ask(|actor_ref| {SettableMsg::Get(actor_ref) }).await;
                output.write().unwrap().replace(get_result);
            });
        }

        sleep(Duration::from_millis(100)).await;
        let current = output.read().unwrap().clone();
        assert_eq!(current, None);

        system.tell(SettableMsg::Set(42)).await;

        sleep(Duration::from_millis(100)).await;
        let current = output.read().unwrap().clone();
        assert_eq!(current, Some(42));
    }
}