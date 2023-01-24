use std::sync::Arc;
use std::path::PathBuf;
use std::process::exit;
use acquisition::protocol;
use crate::task::{DeepCrawlTask, Task};
use crate::controller::{Controller, OneTimeUse};
use calico_shared::result::CalicoResult;
use chrono::DateTime;
use clap::*;
use log::{error, info};

use object_store::{local::LocalFileSystem, ObjectStore};
use storelib::{log::TransactionLog, datatypes::datetime_to_timestamp};
use tempfile::tempdir;
use tokio::fs::create_dir_all;

mod controller;
mod fetch;
mod smarts;
mod state;
mod store;
mod task;
mod telemetry;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct CrawlerCLI {
    #[clap(flatten)]
    verbose: clap_verbosity_flag::Verbosity,

    #[arg(long="local")]
    local_path: Option<String>,

    #[arg(long = "store", value_enum, default_value_t = ObjectStoreType::Local)]
    object_store_type: ObjectStoreType,

    #[arg(long)]
    local_store_path: Option<String>,

    #[arg(long)]
    remote_prefix: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum ObjectStoreType {
    Local,
    S3,
}

#[derive(Subcommand)]
pub(crate) enum Commands {
    /// Deep Crawl a domain
    Deep { 
        domain: String,

        #[arg(short, long, default_value_t = true)]
        use_sitemap: bool,

        #[arg(short, long = "rate", default_value_t = 100)]
        fetch_rate_ms: u64,

        #[arg(short, long, default_value_t = 1000)]
        num_to_fetch: u64,

        #[arg(short, long, default_value_t = 60*60*1000)]
        time_to_fetch: u64,

        #[arg(short, long)]
        seed_list: Vec<String>,

        #[arg(long)]
        seed_list_file: Option<String>,

        #[arg(short, long)]
        min_changed_date: Option<String>,
    },

    /// Perform a wide-crawl
    WideCrawl { seed: Vec<String>} 
}

async fn init_object_store_from_args(cli: &CrawlerCLI, fallback_path: &PathBuf) -> CalicoResult<Arc<dyn ObjectStore>> 
{
    match cli.object_store_type {
        ObjectStoreType::Local => {
            let local_path:PathBuf = match cli.local_store_path.as_ref() {
                Some(path) => path.into(),
                None => {
                    let mut fallback_path = fallback_path.clone();
                    fallback_path.push("local_store");
                    
                    create_dir_all(&fallback_path).await?;
                    
                    fallback_path
                }
            };
            Ok(Arc::new(LocalFileSystem::new_with_prefix(local_path)?))
        },
        ObjectStoreType::S3 => {
            todo!()
        }
    }
}



pub(crate) fn init_controller_from_args(command: &Commands) -> Box<dyn Controller> {
    match &command {
        Commands::Deep { 
            domain,
            use_sitemap,
            num_to_fetch,
            time_to_fetch,
            seed_list,
            seed_list_file: _,
            min_changed_date,
            fetch_rate_ms,
        } => {
            info!("Deep Crawl with {}", domain);

            let host = protocol::Host {
                hostname: domain.to_string(),
                ..Default::default()
            };

            let min_changed_date = match min_changed_date {
                Some(date) => {
                    let datetime = DateTime::parse_from_rfc3339(date).unwrap().with_timezone(&chrono::Utc);
                    datetime_to_timestamp(&datetime)
                },
                None => 0
            };

            let task:Box<dyn Task + Send> = Box::new(DeepCrawlTask {
                domain: host,
                fetch_rate_ms: fetch_rate_ms.clone(),
                use_sitemap: use_sitemap.clone(),
                num_to_fetch: num_to_fetch.clone(),
                time_to_fetch: time_to_fetch.clone(),
                seed_list: seed_list.clone(),
                min_changed_date,
            });

            Box::new(OneTimeUse::init(task))
        }
        _ => todo!(),
    }
}


#[tokio::main]
async fn main() {
    let fallback_tmp = tempdir().unwrap();
    let cli = CrawlerCLI::parse();
    
    env_logger::Builder::new()
        .filter_level(cli.verbose.log_level_filter())
//        .filter_module(module, LevelFilter::Error)
        .init();

    let mut controller = init_controller_from_args(&cli.command);
    let object_store = init_object_store_from_args(&cli, &fallback_tmp.path().to_path_buf()).await.unwrap_or_else(|err|{
        error!("Failed to initialize object store with {}", err);
        exit(1);
    });

    let local_path = match cli.local_path {
        Some(path) => path.into(),
        None => {
            let mut fallback_path = fallback_tmp.path().to_path_buf();
            fallback_path.push("working_dir");
            fallback_path
        }
    };

    let remote_prefix:PathBuf = match cli.remote_prefix {
        Some(prefix) => prefix.into(),
        None => "crawler_state".into()
    };


    // todo: move transaction log to the task somehow. it's potentially a task specific thing
    let transaction_log = Arc::new(TransactionLog::init(object_store.clone()).await.unwrap_or_else(|err|{
        error!("Failed to initialize transaction log with {}", err);
        exit(1);
    }));
    
    // Simple single thread task loop
    // todo: make this support running multiple tasks simultaneously
    // todo: make this support limiting total resources by expected throughput per task
    loop {
        match controller.next_task().await {
            Some(mut task) => {
                let _report = task.run(&local_path, &remote_prefix, object_store.clone(), transaction_log.clone()).await.unwrap_or_else(|err|{
                    error!("Failed to initialize object store with {:?}", err);
                    exit(1);
                });
                ()
            },
            None => break,
        }
    }
}
