use tonic::{transport::Server, Request, Response, Status};

pub mod porta_api {
    tonic::include_proto!("porta_api"); // The string specified here must match the proto package name
}

use porta_api::porta_server::{Porta, PortaServer};
use porta_api::{MetricRequest, MetricResponse};

#[derive(Debug, Default)]
pub struct PortaService {}

#[tonic::async_trait]
impl Porta for PortaService {
    async fn get_metrics(
        &self,
        request: Request<MetricRequest>, // Accept request of type MetricRequest
    ) -> Result<Response<MetricResponse>, Status> { // Return an instance of type MetricResponse
        println!("Got a request: {:?}", request);

        let reply = MetricResponse {
            message: format!("Hello {}!", request.into_inner().metric_suri), // We must use .into_inner() as the fields of gRPC requests and responses are private
        };

        Ok(Response::new(reply)) // Send back our formatted greeting
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let porta = PortaService::default();

    println!("Starting service on {:?}", addr);
    Server::builder()
        .add_service(PortaServer::new(porta))
        .serve(addr)
        .await?;

    Ok(())
}