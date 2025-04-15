use clap::{Parser, Subcommand, Args};
use clap_verbosity_flag::{ Verbosity, InfoLevel };
use env_logger::Builder;
use log::{debug, error, info };
use tonic::transport::Channel;
use tonic::{Request, Streaming, Status};
use std::collections::HashMap;
use std::error::Error;

pub mod vera_api {
    tonic::include_proto!("vera_api");
}

#[derive(Debug, thiserror::Error)]
enum VeraError {
    #[error("invalid namespace: {0}")]
    InvalidNamespace(String),

    #[error("invalid key-value: {0}")]
    InvalidKeyVal(String),
}

use vera_api::{
    vera_client::VeraClient,

    WriteDocumentsRequest,
    WriteDocumentsResponse,
    WriteDocumentRequest,
};

#[derive(Parser)]
#[command(name = "Vera CLI")]
#[command(about = "Command line interface for the Vera gRPC service")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// gRPC server address (e.g., "[::1]:50052" or "127.0.0.1:50052")
    #[arg(short = 'g', long, default_value = "[::1]:50053")]
    grpc_address: String,

    /// Verbose output
    #[command(flatten)]
    verbose: Verbosity<InfoLevel>,
}

#[derive(Args, Clone, Debug)]
struct ObjectProperties {
}


#[derive(Subcommand)]
enum Commands {
    Put {
        #[arg(long = "ns", value_parser = parse_key_val_map::<String, String>)]
        namespace: HashMap<String, String>,

        id: String,

        #[arg(value_parser = parse_key_val_map::<String, String>)]
        properties: HashMap<String, String>,    
    },
    Get {
        #[arg(long="ns", value_parser = parse_key_val_map::<String, String>)]
        namespace: HashMap<String, String>,

        id: String,

        #[arg(value_parser = parse_key_val_map::<String, String>)]
        properties: HashMap<String, String>,    
    },
}

fn parse_key_val_map<T, U>(s: &str) -> Result<HashMap<T, U>, Box<dyn Error + Send + Sync + 'static>>
where
    T: std::str::FromStr + std::cmp::Eq + std::hash::Hash,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let mut map = HashMap::new();
    for pair in s.split(',') {
        let (key, value) = parse_key_val(pair)?;
        map.insert(key, value);
    }
    Ok(map)
}

/// Parse a single key-value pair
fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

fn resolve_namespace(
    namespace: &HashMap<String, String>, 
    properties: &HashMap<String, String>
) -> Result<HashMap<String, String>, VeraError> {
    let mut resolved_properties = HashMap::new();
    for (key, value) in properties {
        if let Some(pos) = key.find(':') {
            let namespace_key = &key[..pos];
            let property_key = &key[pos + 1..];
            let resolved_prefix = namespace
                .get(namespace_key)
                .ok_or_else(|| VeraError::InvalidNamespace(namespace_key.to_string()))?;

            let mut resolved_key = resolved_prefix.to_string();
            if resolved_prefix.ends_with('/') {
                resolved_key.push_str(property_key);
            } else {
                resolved_key.push_str(format!("/{}", property_key).as_str());
            }
            resolved_properties.insert(resolved_key.to_string(), value.to_string());
        } else {
            resolved_properties.insert(key.to_string(), value.to_string());
        }
    }
    Ok(resolved_properties)
}

#[tokio::main]
async fn main() -> Result<(), VeraError> {

    let cli = Cli::parse();
    
//    let client = VeraClient::connect(format!("http://{}", cli.grpc_address)).await?;

    match &cli.command {
        Commands::Put { namespace, id, properties } => {
            println!("Put {:?}", namespace);
            println!("Put {:?}", id);
            let resolved_properties = resolve_namespace(namespace, properties)?;
            for (key, value) in resolved_properties {
                println!("-- {:?}={:?}", key, value);
            }
        }
        Commands::Get { namespace, id, properties } => {
            println!("Get {:?}", namespace);
            println!("Get {:?}", id);
            for (key, value) in properties {
                println!("-- {:?}={:?}", key, value);
            }
        }
    }
    Ok(())
}
// test resolve_namespace
#[test]
fn test_resolve_namespace() {
    let namespace = HashMap::from([
        ("ns1".to_string(), "prefix1/".to_string()),
        ("ns2".to_string(), "prefix2/".to_string()),
        ("ns3".to_string(), "prefix3/".to_string())
    ]);
    let properties = HashMap::from([
        ("ns1:key1".to_string(), "value1".to_string()),
        ("ns2:key2".to_string(), "value2".to_string()),
        ("ns3:key3".to_string(), "value3".to_string()),
        ("key4".to_string(), "value4".to_string())
    ]);
    let resolved_properties = resolve_namespace(&namespace, &properties).unwrap();
    assert_eq!(resolved_properties.len(), 4);
    assert_eq!(resolved_properties.get("prefix1/key1").unwrap(), "value1");
    assert_eq!(resolved_properties.get("prefix2/key2").unwrap(), "value2");
    assert_eq!(resolved_properties.get("prefix3/key3").unwrap(), "value3");
    assert_eq!(resolved_properties.get("key4").unwrap(), "value4");
}