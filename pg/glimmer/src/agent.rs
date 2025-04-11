use actix::prelude::*;

enum AgentMessage {
    GetProperties(Recipient<HashMap<String, String>>),
}

struct AgentActor {
    properties: HashMap<String, String>,
}

impl Actor for AgentActor {
    type Context = Context<Self>;
}