use clap::{Parser, Subcommand};
use clap_verbosity_flag::{ Verbosity, InfoLevel };
use env_logger::Builder;
use log::{debug, error, info };
use std::collections::HashMap;
use glimmer::{
    GlimmerConnection,
};

#[derive(Parser)]
#[command(name = "Glimmer CLI")]
#[command(about = "Command line interface for the Glimmer gRPC service")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// gRPC server address (e.g., "[::1]:50052" or "127.0.0.1:50052")
    #[arg(short = 'g', long, default_value = "[::1]:50052")]
    grpc_address: String,

    /// Verbose output
    #[command(flatten)]
    verbose: Verbosity<InfoLevel>,
}

#[derive(Subcommand)]
enum Commands {
    Agents,
    Agent {
        #[command(subcommand)]
        command: Option<AgentCommands>,

        agent_id: String, 
    },
}

#[derive(Subcommand)]
enum AgentCommands {
    Create {
        #[arg(long)]
        template_uri: Option<String>,

    },

    Get {
        property_id: String,
    },

    Set {
        property_id: String,
        value: String,
    },
    Start,
    Pause,
    
}
async fn list_agents(
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Listing agents...");
    Ok(())
}

async fn create_agent(
    grpc_connection: &mut GlimmerConnection,
    agent_id: String, 
    template_uri: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {

    let override_parameters = HashMap::new();

    info!("Creating agent with id: {}", agent_id);
    info!("Template URI: {:?}", template_uri);
    info!("Override parameters: {:?}", override_parameters);   

    let response = grpc_connection.create_agent(agent_id, template_uri, override_parameters).await?;
    info!("Agent created: {:?}", response);
    Ok(())
}

async fn get_agent(
    grpc_connection: &mut GlimmerConnection,
    id: String,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Reading agent with id: {}", id);
    let response = grpc_connection.get_agent(id).await?;
    info!("Agent read: {:?}", response);

    if let Some(agent) = response.agent {
        println!("{}: [{:?}]", agent.id, agent.status);
        if agent.properties.len() > 0 {
            println!("  Properties:");
            for (key, value) in agent.properties {
                println!("    {}: {}", key, value);
            }
        }
    } else {
        println!("Agent not found");
    }
    Ok(())
}

async fn get_property(
    grpc_connection: &mut GlimmerConnection,
    id: String,
    property_name: String,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Getting property '{}' for agent '{}'", property_name, id);
    let response = grpc_connection.get_property(id, property_name).await?;
    info!("Property read: {:?}", response);
    Ok(())
}

async fn set_property(
    grpc_connection: &mut GlimmerConnection,
    id: String, 
    property_name: String, 
    property_value: String
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Setting property '{}' to '{}' for agent '{}'", property_name, property_value, id);
    let response = grpc_connection.set_property(id, property_name, property_value).await?;
    info!("Property set: {:?}", response);
    Ok(())
}

async fn start_agent(
    grpc_connection: &mut GlimmerConnection,
    id: String,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting agent '{}'", id);
    let response = grpc_connection.start_agent(id).await?;
    info!("Agent started: {:?}", response);
    Ok(())
}

async fn pause_agent(
    grpc_connection: &mut GlimmerConnection,
    id: String,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Pausing agent '{}'", id);
    let response = grpc_connection.pause_agent(id).await?;
    info!("Agent paused: {:?}", response);
    Ok(())
}

// glimmer agents
// glimmer agent :my-agent create --template-uri std/chatbot/1.0.0 --override-parameters property1=value1 property2=value2
// glimmer agent :my-agent set :property1 :value1
// glimmer agent :my-agent get :property1
// glimmer agent :my-agent start
// glimmer agent :my-agent pause
// glimmer agent :my-agent stop
// glimmer agent :agent-id channel :channel-id
// glimmer agent :agent-id channel :channel-id send :message
// glimmer agent :agent-id channel :channel-id stream
// glimmer agent :agent-id channel :channel-id delete
// glimmer agent :agent-id delete

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    env_logger::Builder::new()
        .filter_level(cli.verbose.log_level_filter())
        .init();

    info!("Connecting to gRPC server at {}", cli.grpc_address);

    let mut grpc_connection = GlimmerConnection::connect(&cli.grpc_address).await?;

    match cli.command {
        Commands::Agents => {
            list_agents().await?;
        }
        Commands::Agent { command, agent_id } => {
            match command {
                Some(AgentCommands::Create { template_uri }) => {
                    create_agent(&mut grpc_connection, agent_id, template_uri).await?;
                }
                Some(AgentCommands::Get { property_id }) => {
                    get_property(&mut grpc_connection, agent_id, property_id).await?;
                }
                Some(AgentCommands::Set { property_id, value }) => {
                    set_property(&mut grpc_connection, agent_id, property_id, value).await?;
                }
                Some(AgentCommands::Start) => {
                    start_agent(&mut grpc_connection, agent_id).await?;
                }
                Some(AgentCommands::Pause) => {
                    pause_agent(&mut grpc_connection, agent_id).await?;
                }
                None => {
                    get_agent(&mut grpc_connection, agent_id).await?;
                }
            }
        }
    }
    Ok(())
}
