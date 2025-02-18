pub mod vera_api {
    tonic::include_proto!("vera_api"); // The string specified here must match the proto package name
}

use scylla::{Session, SessionBuilder};
use std::time::Duration;

use vera_api::vera_server::{ Vera, VeraServer };
use vera_api::{ReadDocumentsRequest, ReadDocumentsResponse, WriteDocumentsRequest, WriteDocumentsResponse };
use tonic::{transport::Server, Request, Response, Status};

#[derive(Debug, Default)]
pub struct VeraService {}

#[tonic::async_trait]
impl Vera for VeraService {
    async fn get(
        &self,
        request: Request<ReadDocumentsRequest>, // Accept request of type MetricRequest
    ) -> Result<Response<ReadDocumentsResponse>, Status> { // Return an instance of type MetricResponse
        let response = ReadDocumentsResponse {};

        Ok(Response::new(response))
    }

    async fn put(
        &self,
        request: Request<WriteDocumentsRequest>, // Accept request of type MetricRequest
    ) -> Result<Response<WriteDocumentsResponse>, Status> { // Return an instance of type MetricResponse
        let response: WriteDocumentsResponse = WriteDocumentsResponse {};

        Ok(Response::new(response))
    }
}

async fn healthcheck() -> Result<(), Box<dyn std::error::Error>> {
    let uri = std::env::var("SCYLLA_URI")
        .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting Session");
    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .connection_timeout(Duration::from_secs(3))
        .cluster_metadata_refresh_interval(Duration::from_secs(10))
        .build()
        .await?;
    
    println!("Session Connected");
    let result = session
        .query_unpaged("SELECT keyspace_name FROM system_schema.keyspaces;", ())
        .await?
        .into_rows_result()?;
        
    println!("KEYSPACES");
    for row in result.rows::<(Option<&str>,)>()? {
        let (keyspace_name,): (Option<&str>,) = row?;
        println!("{}", keyspace_name.unwrap_or("NONE"));
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    healthcheck().await?;

    let addr = "[::1]:50052".parse()?;
    let vera = VeraService::default();
    println!("{} Starting VeraService on {:?}", "🦑", addr);
    Server::builder()
        .add_service(VeraServer::new(vera))
        .serve(addr)
        .await?;

    Ok(())
}
