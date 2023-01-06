use std::{path::Path, time::Instant, task::Poll};

use acquisition::result::IndigoResult;
use async_trait::async_trait;
use futures::{StreamExt, future};

use crate::fetch::{retrieve};

use super::{DomainState, Host, frontier::{LastVisitStore, FrontierStore }};


#[async_trait]
pub trait Task {
    async fn run(&mut self) -> IndigoResult<TaskReport>;
}

/// Frontier task uses a queue & prioritization function to recursively crawl a domain.
#[derive(Clone, Debug, Default)]
pub struct DeepCrawlTask {
    pub domain: Host,

    /// The period in between fetch requests to wait
    pub fetch_rate_ms: u64,

    /// 
    pub use_sitemap: bool,

    pub num_to_fetch: u64,

    pub time_to_fetch: u64,

    pub seed_list: Vec<String>
}

pub struct TaskReport {
    pub pages_fetched: u64,
}

#[async_trait]
impl Task for DeepCrawlTask {
    async fn run(&mut self) -> IndigoResult<TaskReport> {
        let mut pages_fetched = 0;
        let path = Path::new("/tmp/.crawler_state");
        let mut domain_state = DomainState::for_domain(&self.domain, self.fetch_rate_ms).await?;
        let now = Instant::now();
        
        let frontier_path = path.join(&self.domain.hostname).join("frontier");
        let mut frontier = FrontierStore::init_local(&frontier_path);

        let last_visit_path = path.join(&self.domain.hostname).join("last_visit");
        let mut _last_visit = LastVisitStore::init_local(&last_visit_path);

        // todo: handle sitemap

        if self.seed_list.len() == 0 {
            self.seed_list.push("/".to_string());
        }
        frontier.sender.append_paths(&self.seed_list).await?;

        // Setup a future to stop the task when a certain amount of time has passed
        let stop_fut = future::poll_fn(|_cx| {
            if now.elapsed().as_millis() > self.time_to_fetch.into() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        });

        let mut url_stream = frontier.receiver
            .take(self.num_to_fetch as usize);
    
            
        while let Some(result) = url_stream.next() {
            match result {
                Ok(url) => {
                    let full_url = self.domain.full_url(&url);
                    match retrieve(&mut domain_state, full_url).await {
                        Ok(capture) => {
                            frontier.sender.append_paths(&capture.outlinks).await?;
                            pages_fetched += 1;
                        },
                        Err(err) => {
                            println!("error = {:?}", err);
                        }
                    };

                }
                Err(err) => {
                    println!("error = {:?}", err);
                    break;
                }
            }
        }

        Ok(TaskReport {
            pages_fetched
        })
    }
}
// todo: implement a widecrawl task
// todo: implement a refresh task


#[cfg(test)]
mod tests {
    use super::super::tests::*;

    use super::{DeepCrawlTask, Task};

    #[tokio::test]
    async fn test_basic_deep_crawl() {
        let _m = [
            with_friendly_robots(),
            with_spider_trap()
        ];

        let mut task = DeepCrawlTask {
            domain: mockito_host(),
            fetch_rate_ms: 0,
            num_to_fetch: 100,
            time_to_fetch: 5000,
            use_sitemap: false,
            seed_list: vec!["/req/abc".to_string()]
        };

        let report = task.run().await.expect("Shouldn't fail");

        assert_eq!(report.pages_fetched, 100);
    }
}
