use std::mem::replace;
use std::time::Duration;

use async_trait::async_trait;
use clap::{Parser, Subcommand};

use crate::fetch::{frontier::{FrontierStore, LastVisitStore}, task::DeepCrawlTask, Host};

use super::task::Task;


#[async_trait]
pub trait Controller {
    async fn next_task(& mut self) -> Option<Box<dyn Task + Send>>;
}


#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct CrawlerCLI {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Deep Crawl a domain
    Deep { 
        domain: String,

        #[arg(short, long, default_value_t = true)]
        use_sitemap: bool

    },

    /// Perform a wide-crawl
    WideCrawl { seed: Vec<String>} 
}

pub fn parse_cli_controller() -> Box<dyn Controller> {
    let cli = CrawlerCLI::parse();

    match &cli.command {
        Commands::Deep { 
            domain,
            use_sitemap
        } => {
            println!("'deep' was used, domain is: {:?}", domain);
            let host = Host {
                hostname: domain.to_string(),
                ..Default::default()
            };

            let fetch_rate_ms = Duration::from_secs(1).as_millis().try_into().expect("Must not wait > 100M years");

            let task:Box<dyn Task + Send> = Box::new(DeepCrawlTask {
                domain: host,
                fetch_rate_ms,
                use_sitemap: use_sitemap.clone(),
                ..Default::default()
            });

            Box::new(OneTimeUse::init(task))
        }
        Commands::WideCrawl { seed } => todo!(),
    }
}

struct OneTimeUse<T>(Option<T>);

impl <T> OneTimeUse<T> {
    pub fn init(x:T) -> OneTimeUse<T> {
        OneTimeUse(Some(x))
    }
}

impl <T> Iterator for OneTimeUse<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_none() {
            None
        } else {
            replace(&mut self.0, None)
        }
    }
}

#[async_trait]
impl Controller for OneTimeUse<Box<dyn Task + Send>> {
    async fn next_task(& mut self) -> Option<Box<dyn Task + Send>> {
        self.next()
    }
}



#[cfg(test)]
use mockito;

#[cfg(test)]
mod tests {
    use super::OneTimeUse;

    #[test]
    fn test_one_time_use() {
        let mut o = OneTimeUse::init(1);
        assert_eq!(o.next(), Some(1));
        assert_eq!(o.next(), None);

        let mut o = OneTimeUse::init(1);
        let v:Vec<u32> = o.collect();
        assert_eq!(v, vec![1]);
    }
}
