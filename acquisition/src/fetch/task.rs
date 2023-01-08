use std::{path::Path, time::{Instant, UNIX_EPOCH, SystemTime} };

use calico_shared::result::CalicoResult;
use async_trait::async_trait;
use itertools::Itertools;

use crate::fetch::{retrieve};
use acquisition::protocol;

use super::{DomainState, frontier::{LastVisitStore, FrontierStore }, full_url, Capture};
use crate::telemetry;

#[async_trait]
pub trait Task {
    async fn run(&mut self) -> CalicoResult<TaskReport>;
}

/// Frontier task uses a queue & prioritization function to recursively crawl a domain.
#[derive(Clone, Debug, Default)]
pub struct DeepCrawlTask {
    pub domain: protocol::Host,

    /// The period in between fetch requests to wait
    pub fetch_rate_ms: u64,

    /// 
    pub use_sitemap: bool,

    pub num_to_fetch: u64,

    pub time_to_fetch: u64,

    pub seed_list: Vec<String>
}

#[derive(Clone, Debug, Default)]
pub struct TaskReport {
    pub pages_fetched: u64,
    pub avg_latency: f64,
    pub avg_size: f64,
    pub max_size: f64
}

type CrawlStat=Box<dyn telemetry::Accumulator<f64> + Send>;

pub struct CrawlTelementry {
    
    avg_latency: CrawlStat,
    avg_size: CrawlStat,
    max_size: CrawlStat,
    avg_outlinks: CrawlStat,
    max_outlinks: CrawlStat
}

impl CrawlTelementry {
    pub fn init() -> CrawlTelementry {
        let now:f64 = SystemTime::now().duration_since(UNIX_EPOCH)
            .expect("Must be able to get current time").as_secs() as f64;
            
        CrawlTelementry { 
            avg_latency: Box::new(telemetry::ExponentialDecayAccumulator::init(0.001, now)),
            avg_size: Box::new(telemetry::SumAccumulator::default()),
            max_size: Box::new(telemetry::MaxAccumulator::default()),
            avg_outlinks: Box::new(telemetry::SumAccumulator::default()),
            max_outlinks: Box::new(telemetry::MaxAccumulator::default()),
        }
    }

    pub fn add(&mut self, capture: &Capture) {
        let now:f64 = SystemTime::now().duration_since(UNIX_EPOCH)
            .expect("Must be able to get current time").as_secs() as f64;

        self.avg_latency.add(capture.fetch_time as f64, 1., now);
        self.avg_size.add(capture.content_length as f64, 1., now);
        self.max_size.add(capture.content_length as f64, 1., now);
        self.avg_outlinks.add(capture.outlinks.len() as f64, 1., now);
        self.max_outlinks.add(capture.outlinks.len() as f64, 1., now);
    }
}

#[async_trait]
impl Task for DeepCrawlTask {
    async fn run(&mut self) -> CalicoResult<TaskReport> {
        let mut pages_fetched = 0;
        let path = Path::new("/tmp/.crawler_state");
        let mut domain_state = DomainState::for_domain(&self.domain, self.fetch_rate_ms).await?;
        let now = Instant::now();
        
        let frontier_path = path.join(&self.domain.hostname).join("frontier");
        let mut frontier = FrontierStore::init_local(&frontier_path);

        let last_visit_path = path.join(&self.domain.hostname).join("last_visit");
        let mut last_visit = LastVisitStore::init_local(&last_visit_path)?;

        let mut telemetry = CrawlTelementry::init();

        // todo: handle sitemap

        if self.seed_list.len() == 0 {
            self.seed_list.push("/".to_string());
        }
        frontier.sender.append_paths(&self.seed_list).await?;

        let mut url_stream = frontier.receiver
            .take(self.num_to_fetch as usize)
            .take_while(|_| now.elapsed().as_millis() < self.time_to_fetch.into());
            
        while let Some(result) = url_stream.next() {
            match result {
                Ok(url) => {
                    let full_url = full_url(&self.domain, &url);
                    match retrieve(&mut domain_state, full_url).await {
                        Ok(capture) => {
                            telemetry.add(&capture);

                            let unique_and_new:Vec<_> = capture.outlinks.into_iter()
                                .unique()
                                .filter(|link| !last_visit.contains(link).unwrap_or(false))
                                .collect();
                            
                            let now:u64 = SystemTime::now().duration_since(UNIX_EPOCH)
                                .expect("Time went backwards")
                                .as_millis()
                                .try_into()
                                .expect("Time is millions of years in the future");
                            
                            for link in &unique_and_new {
                                let entry = protocol::LastVisit {
                                    added_to_frontier: now,
                                    fetched: None,
                                    status_code: None
                                };

                                last_visit.set(&link, entry)?;  
                            }
                
                            frontier.sender.append_paths(&unique_and_new).await?;
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
            pages_fetched,
            avg_latency: telemetry.avg_latency.value(),
            avg_size: telemetry.avg_size.value(),
            max_size: telemetry.max_size.value()
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
    async fn test_basic_deep_crawl_num_fetch_stop() {
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

        println!("Report: {:?}", report);
        assert_eq!(report.pages_fetched, 100);
        assert!(report.avg_latency > 0.);
        assert!(report.avg_size > 0.);
        assert!(report.max_size > 0.);
    }
    #[tokio::test]
    async fn test_basic_deep_crawl_timer_stop() {
        let _m = [
            with_friendly_robots(),
            with_spider_trap()
        ];

        let mut task = DeepCrawlTask {
            domain: mockito_host(),
            fetch_rate_ms: 0,
            num_to_fetch: 1000,
            time_to_fetch: 50,
            use_sitemap: false,
            seed_list: vec!["/req/abc".to_string()]
        };

        let report = task.run().await.expect("Shouldn't fail");

        assert!(report.pages_fetched < 1000);
    }
}
