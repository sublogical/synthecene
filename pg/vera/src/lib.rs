pub mod vera_api {
    tonic::include_proto!("vera_api");
}

use vera_api::vera_client::VeraClient;
use tonic::transport::Channel;
use tonic::{Request, Streaming, Status};
use std::collections::HashMap;
use vera_api::{
    WriteDocumentsRequest,
    WriteDocumentsResponse,
};

