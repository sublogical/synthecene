use clap::{Parser, Subcommand, Args};
use clap_verbosity_flag::{ Verbosity, InfoLevel };
use log::{error, info };
use std::collections::HashMap;
use std::error::Error;

pub mod vera_api {
    tonic::include_proto!("vera_api");
}

#[derive(Debug, thiserror::Error)]
enum VeraError {
    #[error("invalid namespace: {0}")]
    InvalidNamespace(String),

    #[error("invalid column values: {0}")]
    InvalidColumnValues(String),

    #[error("failed to connect to server: {0}")]
    TonicError(#[from] tonic::transport::Error),

    #[error("failed to call RPC method: {0}")]
    GrpcError(#[from] tonic::Status),

}

use vera_api::{
    vera_client::VeraClient,
    CellValue,
    CreateTableRequest,
    ColumnSpec,
    DeleteTableRequest,
    DocumentUpdate,
    WriteDocumentsRequest,
    cell_value::Data,
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
        namespace: Option<HashMap<String, String>>,

        universe_uri: String,

        table_uri: String,

        document_uri: String,

        #[arg(value_parser = parse_json_value::<serde_json::Value>)]
        column_values: serde_json::Value,
    },
    Get {
        #[arg(long="ns", value_parser = parse_key_val_map::<String, String>)]
        namespace: Option<HashMap<String, String>>,

        universe_uri: String,

        table_uri: String,

        document_uri: String,

        column_uris: Vec<String>,    
    },

    CreateTable {
        #[arg(long="ns", value_parser = parse_key_val_map::<String, String>)]
        namespace: Option<HashMap<String, String>>,

        universe_uri: String,

        table_uri: String,

        #[arg(value_parser = parse_key_val_map::<String, String>)]
        column_config: HashMap<String, String>,    
    },

    DeleteTable {
        #[arg(long="ns", value_parser = parse_key_val_map::<String, String>)]
        namespace: Option<HashMap<String, String>>,

        universe_uri: String,

        table_uri: String,

        #[arg(long = "if-not-empty", default_value = "false")]
        delete_if_not_empty: bool,
    },

    CreateColumn {
        #[arg(long="ns", value_parser = parse_key_val_map::<String, String>)]
        namespace: Option<HashMap<String, String>>,

        universe_uri: String,
        
        table_uri: String,

        column_uri: String,

        #[arg(value_parser = parse_key_val_map::<String, String>)]
        column_config: HashMap<String, String>,    
    },

    DeleteColumn {
        #[arg(long="ns", value_parser = parse_key_val_map::<String, String>)]
        namespace: Option<HashMap<String, String>>,
        
        universe_uri: String,

        table_uri: String,

        column_uri: String,
    },
}

fn parse_json_value<T>(s: &str) -> Result<T, Box<dyn Error + Send + Sync + 'static>>
where
    T: serde::de::DeserializeOwned,
{
    let value = serde_json::from_str::<T>(s)?;
    Ok(value)
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

const DEFAULT_NAMESPACES: &[(&'static str, &'static str)] = &[
    ("std", "/std/"),
];

fn compute_default_namespaces() -> HashMap<String, String> {
    DEFAULT_NAMESPACES.iter().map(|(key, value)| {
        (key.to_string(), value.to_string())
    }).collect()
}

fn compute_namespace_map(
    namespace: &Option<HashMap<String, String>>,
) -> HashMap<String, String> {
    let mut namespace_map = compute_default_namespaces();
    if let Some(namespace) = namespace {
        namespace_map.extend(namespace.iter().map(|(key, value)| {
            (key.to_string(), value.to_string())
        }));
    }
    namespace_map
}

fn resolve_namespace(
    namespace: &HashMap<String, String>,
    item: &str
) -> Result<String, VeraError> {
    if let Some(pos) = item.find(':') {
        let namespace_key = &item[..pos];
        let property_key = &item[pos + 1..];
        let resolved_prefix = namespace
            .get(namespace_key)
            .ok_or_else(|| VeraError::InvalidNamespace(namespace_key.to_string()))?;

        let mut resolved_item = resolved_prefix.to_string();
        if resolved_prefix.ends_with('/') {
            resolved_item.push_str(property_key);
        } else {
            resolved_item.push_str(format!("/{}", property_key).as_str());
        }
        Ok(resolved_item)
    } else {
        Ok(item.to_string())
    }
}

fn resolve_namespace_vec(
    namespace: &HashMap<String, String>, 
    vec: &Vec<String>) -> Result<Vec<String>, VeraError> {

    let resolved_items : Result<Vec<_>, _>= vec.iter().map(|item| {
        resolve_namespace(namespace, item)
    }).collect();

    resolved_items
}

fn resolve_namespace_map(
    namespace: &HashMap<String, String>, 
    map: &HashMap<String, String>,
    resolve_keys: bool,
    resolve_values: bool
) -> Result<HashMap<String, String>, VeraError> {
    let keys = if resolve_keys {
        resolve_namespace_vec(namespace, &map.keys().cloned().collect())?
    } else {
        map.keys().cloned().collect()
    };
    let values = if resolve_values {
        resolve_namespace_vec(namespace, &map.values().cloned().collect())?
    } else {
        map.values().cloned().collect()
    };

    let resolved_map: HashMap<_, _> = keys.iter().zip(values.iter()).map(|(key, value)| {
        (key.to_string(), value.to_string())
    }).collect();
    Ok(resolved_map)
}

#[tokio::main]
async fn main() -> Result<(), VeraError> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("vera=debug")
        .init();

    let cli = Cli::parse();
    info!("Connecting to {:?}", cli.grpc_address);
    let mut client = VeraClient::connect(format!("http://{}", cli.grpc_address)).await?;
    
    match &cli.command {
        Commands::Put { namespace, universe_uri, table_uri, document_uri, column_values } => {
            let namespace_map = compute_namespace_map(&namespace);

            let serde_json::Value::Object(column_object) = column_values else {
                return Err(VeraError::InvalidColumnValues("column_values is required to be a JSON object".to_string()));
            };

            let resolved_column_uris = resolve_namespace_vec(&namespace_map, &column_object.keys().cloned().collect())?;
            let cell_values = column_object.values().cloned().map(|value| {
                match value {
                    serde_json::Value::String(s) => Ok(CellValue {
                        data: Some(Data::StringValue(s)),
                    }),
                    serde_json::Value::Number(n) => {
                        if n.is_i64() {
                            Ok(CellValue {
                                data: Some(Data::Int64Value(n.as_i64().unwrap())),
                            })
                        } else {
                            Ok(CellValue {
                                data: Some(Data::DoubleValue(n.as_f64().unwrap())),
                            })
                        }
                    },
                    serde_json::Value::Bool(b) => Ok(CellValue {
                        data: Some(Data::BooleanValue(b)),
                    }),
                    serde_json::Value::Array(a) => Err(VeraError::InvalidColumnValues("array values are not supported".to_string())),
                    serde_json::Value::Object(o) => Err(VeraError::InvalidColumnValues("object values are not supported".to_string())),
                    _ => Err(VeraError::InvalidColumnValues("unsupported value type".to_string())),
                }
            }).collect::<Result<Vec<_>, _>>()?;

            let resolved_universe_uri = resolve_namespace(&namespace_map, universe_uri)?;
            let resolved_table_uri = resolve_namespace(&namespace_map, table_uri)?;
            let resolved_document_uri = resolve_namespace(&namespace_map, document_uri)?;
            info!("Put universe:{:?}, table:{:?}, document:{:?}, column_uris:{:?}", resolved_universe_uri, resolved_table_uri, resolved_document_uri, resolved_column_uris);

            let document_updates = vec![DocumentUpdate {
                document_id: resolved_document_uri,
                cell_values: cell_values,
            }];

            let request = WriteDocumentsRequest {
                universe_uri: resolved_universe_uri,
                table_uri: resolved_table_uri,
                column_uris: resolved_column_uris,
                document_updates: document_updates,
            };
            let response = client.put(request).await?;
            info!("Put response: {:?}", response);
        }
        Commands::Get { namespace, universe_uri, table_uri, document_uri, column_uris } => {
            let namespace_map = compute_namespace_map(&namespace);
            info!("Get {:?}", namespace_map);
            info!("Get {:?}", universe_uri);
            info!("Get {:?}", table_uri);
            info!("Get {:?}", document_uri);
            info!("Get {:?}", column_uris);

            let resolved_column_uris = resolve_namespace_vec(&namespace_map, &column_uris)?;
            for column_uri in resolved_column_uris {
                info!("-- {:?}", column_uri);
            }
        }
        Commands::CreateTable { namespace, universe_uri, table_uri, column_config } => {
            let namespace_map = compute_namespace_map(&namespace);
            let resolved_column_config = resolve_namespace_map(&namespace_map, column_config, true, true)?;
            info!("CreateTable universe:{:?}, table:{:?}, column_config:{:?}", universe_uri, table_uri, column_config);

            let resolved_universe_uri = resolve_namespace(&namespace_map, universe_uri)?;
            let resolved_table_uri = resolve_namespace(&namespace_map, table_uri)?;

            let column_specs = resolved_column_config.iter().map(|(key, value)| {
                ColumnSpec {
                    column_uri: key.to_string(),
                    type_uri: value.to_string(),
                }
            }).collect();

            let request = CreateTableRequest {
                universe_uri: resolved_universe_uri,
                table_uri: resolved_table_uri,
                column_specs: column_specs,
            };
            let response = client.create_table(request).await?;
            info!("CreateTable response: {:?}", response);
        }
        Commands::DeleteTable { namespace, universe_uri, table_uri, delete_if_not_empty } => {
            let namespace_map = compute_namespace_map(&namespace);
            info!("DeleteTable universe:{:?}, table:{:?}, delete_if_not_empty:{:?}", universe_uri, table_uri, delete_if_not_empty);

            let resolved_universe_uri = resolve_namespace(&namespace_map, universe_uri)?;
            let resolved_table_uri = resolve_namespace(&namespace_map, table_uri)?;

            let request = DeleteTableRequest {
                universe_uri: resolved_universe_uri,
                table_uri: resolved_table_uri,
                delete_if_not_empty: *delete_if_not_empty,
            };
            let response = client.delete_table(request).await?;
            info!("DeleteTable response: {:?}", response);
        }
        Commands::CreateColumn { namespace, universe_uri, table_uri, column_uri, column_config } => {
            let namespace_map = compute_namespace_map(&namespace);
            info!("CreateColumn {:?}", namespace_map);
            info!("CreateColumn {:?}", universe_uri);
            info!("CreateColumn {:?}", table_uri);
            info!("CreateColumn {:?}", column_uri);
            let resolved_column_config = resolve_namespace_map(&namespace_map, column_config, true, true)?;
            for (key, value) in resolved_column_config {
                info!("-- {:?}={:?}", key, value);
            }
        }
        Commands::DeleteColumn { namespace, universe_uri, table_uri, column_uri } => {
            let namespace_map = compute_namespace_map(&namespace);
            info!("DeleteColumn {:?}", namespace_map);
            info!("DeleteColumn {:?}", universe_uri);
            info!("DeleteColumn {:?}", table_uri);
            info!("DeleteColumn {:?}", column_uri);
        }
    }
    Ok(())
}

#[test]

fn test_resolve_namespace_vec() {
    let namespace = HashMap::from([
        ("ns1".to_string(), "prefix1/".to_string()),
        ("ns2".to_string(), "prefix2/".to_string()),
        ("ns3".to_string(), "prefix3/".to_string())
    ]);

    let vec = vec![
        "ns1:key1".to_string(),
        "ns2:key2".to_string(),
        "ns3:key3".to_string(),
        "key4".to_string()
    ];
    let resolved_items = resolve_namespace_vec(&namespace, &vec).unwrap();
    assert_eq!(resolved_items.len(), 4);
    assert_eq!(resolved_items[0], "prefix1/key1");
    assert_eq!(resolved_items[1], "prefix2/key2");
    assert_eq!(resolved_items[2], "prefix3/key3");
    assert_eq!(resolved_items[3], "key4");
}

// test resolve_namespace
#[test]
fn test_resolve_namespace_map() {
    let namespace = HashMap::from([
        ("ns1".to_string(), "prefix1/".to_string()),
        ("ns2".to_string(), "prefix2/".to_string()),
        ("ns3".to_string(), "prefix3/".to_string())
    ]);
    let properties = HashMap::from([
        ("ns1:key1".to_string(), "ns1:value1".to_string()),
        ("ns2:key2".to_string(), "ns2:value2".to_string()),
        ("ns3:key3".to_string(), "ns3:value3".to_string()),
        ("key4".to_string(), "value4".to_string())
    ]);
    let resolved_properties = resolve_namespace_map(&namespace, &properties, true, false).unwrap();
    assert_eq!(resolved_properties.len(), 4);
    assert_eq!(resolved_properties.get("prefix1/key1").unwrap(), "ns1:value1");
    assert_eq!(resolved_properties.get("prefix2/key2").unwrap(), "ns2:value2");
    assert_eq!(resolved_properties.get("prefix3/key3").unwrap(), "ns3:value3");
    assert_eq!(resolved_properties.get("key4").unwrap(), "value4");

    let resolved_properties = resolve_namespace_map(&namespace, &properties, false, true).unwrap();
    assert_eq!(resolved_properties.len(), 4);
    assert_eq!(resolved_properties.get("ns1:key1").unwrap(), "prefix1/value1");
    assert_eq!(resolved_properties.get("ns2:key2").unwrap(), "prefix2/value2");
    assert_eq!(resolved_properties.get("ns3:key3").unwrap(), "prefix3/value3");
    assert_eq!(resolved_properties.get("key4").unwrap(), "value4");

    let resolved_properties = resolve_namespace_map(&namespace, &properties, true, true).unwrap();
    assert_eq!(resolved_properties.len(), 4);
    assert_eq!(resolved_properties.get("prefix1/key1").unwrap(), "prefix1/value1");
    assert_eq!(resolved_properties.get("prefix2/key2").unwrap(), "prefix2/value2");
    assert_eq!(resolved_properties.get("prefix3/key3").unwrap(), "prefix3/value3");
    assert_eq!(resolved_properties.get("key4").unwrap(), "value4");
}
