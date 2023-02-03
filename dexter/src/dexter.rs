use std::{any::Any, pin::Pin};

use async_trait::async_trait;
use futures::Future;
use serde::{Serialize, de::DeserializeOwned};

pub enum Error {
    NotFound,
    InternalError,
    InputTypeError{
        index: usize, 
        expected: String, 
    },
}

/**
 * Set the current state value for a node in the dexter graph.
 * 
 * set_node_state("dex://dialog:123/user-text", current(), "Hello World")
 */
pub async fn set_node_state<S, T>(node_url: &str, sequence: S, state: T) -> Result<(), Error>
where
    T: Serialize + DeserializeOwned + Clone + 'static,
    S: Ord + Serialize + DeserializeOwned + Clone + 'static,
{
    todo!("set_node_state")
}

pub async fn get_node_state<T, S>(node_url: &str, sequence: u64) -> Result<T, Error>
where
    T: Serialize + DeserializeOwned + Clone + 'static,
{
    todo!("get_node_state")
}

/*
 * Node:
 *  - identifier (unique)
 *    - universe - the universe in which the node exists
 *    - name - the name of the node within the universe
 *    - configuration - as part of identification, can be thought of as query parameters for a URL
 *  - dependencies - other nodes that this node depends on (inputs)
 *  - type - output type of the node
 *  - state - the current state of the node
 *  - logic - the logic that the node executes
 */

 
trait Builder<T,O,S> 
where
    T: Serialize + DeserializeOwned + Clone + 'static,
    O: Ord + Serialize + DeserializeOwned + Clone + 'static,
    S: Serialize + DeserializeOwned + Clone + 'static
{
    /**
     * Creates a node given the node url. The node will consist of an instance of a Compute and an initial state.
     */
    fn build(&self, node_url: &str) -> Result<(Box<dyn Compute<T,O,S>>, S), Error>;
}


struct MyBuilder {}

type ComputeBuilder<T,O,S> = dyn Fn(&str) -> Result<(Box<dyn Compute<T,O,S>>, S), Error>;

async fn concat(seq: u64, state: String, left: &String, right: &String) -> Result<(String, String), Error> {
    Ok((format!("{}{}: {}{}", state, seq, left, right), state))
}



impl Builder<String, u64, String> for MyBuilder {
    fn build(&self, node_url: &str) -> Result<(Box<dyn Compute<String,u64,String>>, String), Error>
    {
        let compute = ComputeContainer2(concat);
        let state = "prefix".to_string();
        Ok((Box::new(compute), state))
    }
}

#[async_trait]
trait Compute<T, O, S> 
where
    T: Serialize + DeserializeOwned + Clone + 'static,
    O: Ord + Serialize + DeserializeOwned + Clone + 'static,
    S: Serialize + DeserializeOwned + Clone + 'static
{
    async fn compute(&self, sequence: O, state: S, inputs: Vec<Box<dyn Any + Send>>) -> Result<(T,S), Error>;
}

trait Compute2<T, O, S, I1, I2> 
where
    T: Serialize + DeserializeOwned + Clone + 'static,
    O: Ord + Serialize + DeserializeOwned + Clone + 'static,
    S: Serialize + DeserializeOwned + Clone + 'static,
    I1: Clone + 'static,
    I2: Clone + 'static,
{
    fn compute2(&self, seq: O, state: S, input1: &I1, input2: &I2) -> Result<(T,S), Error>;
}

struct ComputeContainer2<T, O, S, I1, I2>(ComputeFn2<T, O, S, I1, I2>);

type ComputeFn2<T, O, S, I1, I2> = fn(seq: O, state: S, input1: &I1, input2: &I2) -> Pin<Box<dyn Future<Output = Result<(T,S), Error>> + Send>>;

#[async_trait]
impl <T, O, S, I1, I2> Compute<T, O, S> for ComputeContainer2<T, O, S, I1, I2> 
where
    T: Serialize + DeserializeOwned + Clone  + Send + Sync  + 'static,
    O: Ord + Serialize + DeserializeOwned + Clone + Send + Sync  + 'static,
    S: Serialize + DeserializeOwned + Clone + Send + Sync  + 'static,
    I1: Clone + Send + Sync + 'static,
    I2: Clone + Send + Sync  + 'static,
{
    async fn compute(&self, seq: O, state: S, inputs: Vec<Box<dyn Any + Send>>) -> Result<(T,S), Error> {
        let input1 = inputs[0].downcast_ref::<I1>()
            .ok_or(Error::InputTypeError{
                index: 0,
                expected: std::any::type_name::<I1>().to_string(),
            })?;

        let input2 = inputs[1].downcast_ref::<I2>()
            .ok_or(Error::InputTypeError{
                index: 1,
                expected: std::any::type_name::<I2>().to_string()
            })?;

        self.0(seq, state, input1, input2).await
    }
}





fn compute3<O, I1, I2, I3> (input1: I1, input2: I2, input3: I3) -> O
{
    todo!()
}