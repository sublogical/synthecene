use log::debug;

use super::{ActorRef, Behavior};
use std::fmt::Debug;

#[derive(Debug)]
enum SettableMsg<T> {
    Set(T),
    Get(ActorRef<T>),
}

/**
 * A node that can be set to a value and then retrieved.
 */
pub struct SettableNode;

#[derive(Debug)]
enum Error {
    ResetIsNotAllowed,
}

#[derive(Default)]
struct SettableNodeConfig {
    fail_on_reset: bool,
}

struct SettableNodeBuilder {
    config: SettableNodeConfig,
}

impl SettableNodeBuilder {
    fn new() -> Self {
        Self {
            config: SettableNodeConfig {
                fail_on_reset: false,
            }
        }
    }

    fn fail_on_reset(mut self) -> Self {
        self.config.fail_on_reset = true;
        self
    }
}

impl SettableNode {
    fn configure<T, F>(fun: F) -> Behavior<SettableMsg<T>, Error>
    where
        T: Clone + Debug + Send + Sync + 'static,
        F: FnOnce(SettableNodeBuilder) -> SettableNodeBuilder
    {
        Self::apply(fun(SettableNodeBuilder::new()).config)
    }

    fn default<T>() -> Behavior<SettableMsg<T>, Error>
    where 
        T: Clone + Debug + Send + Sync + 'static
    {
        Self::apply(SettableNodeConfig::default())
    }

    fn apply<T>(config: SettableNodeConfig) -> Behavior<SettableMsg<T>, Error> 
    where 
        T: Clone + Debug + Send + Sync + 'static 
    {
        Self::empty(config, vec![])
    }

    fn empty<T: Clone + Debug + Send + Sync + 'static>(config: SettableNodeConfig, mut pending: Vec<ActorRef<T>>) -> Behavior<SettableMsg<T>, Error> {
        Behavior::running(move |_, msg:SettableMsg<T>| {
            println!("SettableNode::empty: {:?}", msg);
            match msg {
                SettableMsg::Set(value) => {
                    {
                        let value = value.clone();
                        tokio::task::spawn(async move {
                            debug!("SettableNode::empty: notify_pending");
                            Self::notify_pending(&pending, &value).await;
                        });
                    }
                    Self::resolved(config, value)
                }
                SettableMsg::Get(respond_to) => {
                    pending.push(respond_to);

                    Self::empty(config, pending)
                }
            }
        })
    }

    async fn notify_pending<T: Clone + Send + 'static>(pending: &Vec<ActorRef<T>>, value: &T) {
        for respond_to in pending {
            respond_to.tell(value.clone()).await;
        }
        
    }

    fn resolved<T: Clone + Send + Sync + 'static>(config: SettableNodeConfig, value: T) -> Behavior<SettableMsg<T>, Error> {
        Behavior::running(move |_, msg:SettableMsg<T>| {
            match msg {
                SettableMsg::Set(new_value) => {
                    if config.fail_on_reset {
                        Behavior::unrecoverable(Error::ResetIsNotAllowed)
                    } else {
                        Self::resolved(config, new_value)
                    }
                }
                SettableMsg::Get(respond_to) => {
                    {
                        let value = value.clone();
                        tokio::task::spawn(async move {
                            debug!("SettableNode::empty: notify_pending");
                            respond_to.tell(value.clone()).await;
                        });
                    }                        
                    Self::resolved(config, value)
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
            SettableNode::default::<i32>(), "settable_node"));

        let output = Arc::new(RwLock::new(None));
        {
            let output = output.clone();
            let system = system.clone();

            tokio::spawn(async move {
                let get_result = system.ask(|actor_ref| {SettableMsg::Get(actor_ref) }).await.unwrap();
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

    #[tokio::test]
    async fn test_unresettable_node() {
        let system = Arc::new(ActorSystem::new(
            SettableNode::configure::<i32, _>(|builder| builder.fail_on_reset()), "unresettable_node"));

        system.tell(SettableMsg::Set(42)).await;
        system.tell(SettableMsg::Set(52)).await;

        assert!(system.is_stopped());
    }


}