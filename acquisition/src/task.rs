use std::{path::{PathBuf}, time::{Instant, UNIX_EPOCH, SystemTime, Duration}, sync::Arc };

use async_trait::async_trait;
use itertools::Itertools;
use log::{info, error};
use object_store::ObjectStore;
use storelib::log::TransactionLog;
use tokio::time::sleep;

use crate::fetch::{full_url, Capture, robots_filter, retrieve };
use crate::protocol;
use crate::store::LocalStore;
use super::state::{DomainState};
use crate::telemetry;

#[derive(Debug)]
pub enum Error {
    StateFailure(crate::state::Error, &'static str),
    StoreFailure(anyhow::Error, &'static str),
}

pub type Result<T> = std::result::Result<T, Error>;


#[async_trait]
pub trait Task {
    async fn run(&mut self, 
                 local_path:&PathBuf,
                 remote_path:&PathBuf,
                 object_store: Arc<dyn ObjectStore>,
                 transaction_log: Arc<TransactionLog>) -> Result<TaskReport>;
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

    pub seed_list: Vec<String>,

    pub min_changed_date: u64,
}

#[derive(Clone, Debug, Default)]
pub struct TaskReport {
    pub pages_fetched: u64,
    pub avg_latency: f64,
    pub avg_size: f64,
    pub max_size: f64
}

pub struct CrawlTelementry {
    
    avg_latency: telemetry::Stat,
    avg_size: telemetry::Stat,
    max_size: telemetry::Stat,
    avg_outlinks: telemetry::Stat,
    max_outlinks: telemetry::Stat
}

impl CrawlTelementry {
    pub fn init() -> CrawlTelementry {
        let now:f64 = SystemTime::now().duration_since(UNIX_EPOCH)
            .expect("Must be able to get current time").as_secs() as f64;
            
        CrawlTelementry { 
            avg_latency: Box::new(telemetry::ExponentialDecayAccumulator::init(0.001, now)),
            avg_size: Box::new(telemetry::MeanAccumulator::default()),
            max_size: Box::new(telemetry::MaxAccumulator::default()),
            avg_outlinks: Box::new(telemetry::MeanAccumulator::default()),
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
    async fn run(&mut self, 
                 local_path:&PathBuf,
                 remote_path:&PathBuf,
                 object_store: Arc<dyn ObjectStore>,
                 transaction_log: Arc<TransactionLog>) -> Result<TaskReport> 
    {
        let mut pages_fetched = 0;

        let mut local_path = local_path.clone();
        local_path.push(&self.domain.hostname);

        let mut domain_state_path = local_path.clone();
        domain_state_path.push("domain_state");
        
        let mut remote_path = remote_path.clone();
        remote_path.push(&self.domain.hostname);
        
        let mut domain_state = DomainState::init(&self.domain, &domain_state_path, &remote_path, object_store.clone(), transaction_log.clone()).await.expect("should work");
        let now = Instant::now();

        let mut telemetry = CrawlTelementry::init();

        let mut capture_state_path = local_path.clone();
        capture_state_path.push("capture_state");
        
        let mut local_store = LocalStore::init(&self.domain, &capture_state_path).await
            .map_err(|err| Error::StoreFailure(err, "failed to initialize local store"))?;

        let checkpoint_threshold = 1_000;

        // todo: handle sitemap

        if self.seed_list.len() == 0 {
            self.seed_list.push("/".to_string());
        }
        
        fn append_seeds(domain_state: &mut DomainState, seed_list: &Vec<String>) -> Result<()>{
            for seed in seed_list {
                let request = protocol::CaptureRequest { 
                    path: seed.to_string(), 
                    capture_type: protocol::RequestType::Page.into()
                };
                domain_state.frontier.append_message(0, &request)
                    .map_err(|err| Error::StateFailure(err, "failed to append url request to frontier"))?;

                let last_visit = protocol::LastVisit { 
                    added_to_frontier: true,
                    ..Default::default() 
                };

                domain_state.last_visit.put_message(seed.as_bytes(), &last_visit)
                    .map_err(|err| Error::StateFailure(err, "failed to set request state to visit map"))?;
            }
            Ok(())
        }
        append_seeds(&mut domain_state, &self.seed_list)?;

        loop {
            let mut fetched_this_batch = 0;
            let mut new_urls = vec![];
            {
                let mut url_stream = domain_state.frontier.iter_message::<protocol::CaptureRequest>()
                    .take(self.num_to_fetch as usize)
                    .take_while(|_| now.elapsed().as_millis() < self.time_to_fetch.into())
                    .flatten()
                    .filter(|capture_request| robots_filter(&domain_state.robots, &capture_request.path));

                while let Some(capture_request) = url_stream.next() {
                    fetched_this_batch += 1;
                    let full_url = full_url(&self.domain, &capture_request.path);

                    info!("Fetch {}", full_url);

                    match retrieve(full_url).await {
                        Ok(capture) => {
                            telemetry.add(&capture);

                            local_store.add_capture(&capture)
                                .map_err(|err| Error::StoreFailure(err, "failed to add capture to local store"))?;

                            local_store.add_outlinks(&capture.outlinks)
                                .map_err(|err| Error::StoreFailure(err, "failed to add outlinks to local store"))?;
                            
                            new_urls.extend(capture.inlinks);

                            let last_visit = protocol::LastVisit { 
                                added_to_frontier: true,
                                status_code: Some(capture.status_code),
                                fetched_at: Some(capture.fetched_at),
                                ..Default::default() 
                            };
                            domain_state.last_visit.put_message(capture_request.path.as_bytes(), &last_visit)
                                .map_err(|err| Error::StateFailure(err, "failed to set capture state to visit map"))?;
                        }
                        Err(err) => {
                            error!("Error fetching URL: {}", err);
                        }
                    }

                    sleep(Duration::from_millis(self.fetch_rate_ms)).await;
                }
            }

            // todo: filter out urls that are to other hosts
            // todo: rank URLs by priority

            let unique_and_new:Vec<_> = new_urls.into_iter()
                .unique()
                .filter(|link| !domain_state.last_visit.contains(link.as_bytes()).unwrap_or(false))
                .collect();

            append_seeds(&mut domain_state, &unique_and_new)?;
            pages_fetched += fetched_this_batch;

            domain_state.maybe_checkpoint(object_store.clone(), transaction_log.clone(), checkpoint_threshold).await
                .map_err(|err| Error::StateFailure(err, "failed to save domain state"))?;

            local_store.maybe_upload(&remote_path, object_store.clone()).await
                .map_err(|err| Error::StoreFailure(err, "failed to upload local store"))?;

            if fetched_this_batch == 0 {
                break;
            } else {
                self.num_to_fetch -= fetched_this_batch;
            }
        }

        // always checkpoint & upload at the end of a crawl
        local_store.upload(&remote_path, object_store.clone()).await
            .map_err(|err| Error::StoreFailure(err, "failed to upload local store"))?;

        domain_state.checkpoint(object_store.clone(), transaction_log.clone()).await
            .map_err(|err| Error::StateFailure(err, "failed to save domain state"))?;

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
    use std::path::PathBuf;
    use std::sync::Arc;

    use object_store::ObjectStore;
    use object_store::local::LocalFileSystem;
    use storelib::log::TransactionLog;
    use tempfile::tempdir;

    use crate::fetch::tests::*;

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
            time_to_fetch: 100000,
            use_sitemap: false,
            seed_list: vec!["/req/abc".to_string()],
            ..Default::default() 
        };

        let temp_dir = tempdir().unwrap();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let transaction_log = Arc::new(TransactionLog::init(object_store.clone()).await.unwrap());

        let local_dir = tempdir().unwrap();
        let remote_path:PathBuf = "crawler_state".into();
        
        let report = task.run(&local_dir.path().into(), &remote_path, object_store, transaction_log).await.expect("Shouldn't fail");

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
            seed_list: vec!["/req/abc".to_string()],
            ..Default::default() 
        };

        let temp_dir = tempdir().unwrap();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let transaction_log = Arc::new(TransactionLog::init(object_store.clone()).await.unwrap());

        let local_dir = tempdir().unwrap();
        let remote_path:PathBuf = "crawler_state".into();

        let report = task.run(&local_dir.path().into(), &remote_path, object_store, transaction_log).await.expect("Shouldn't fail");

        assert!(report.pages_fetched < 1000);
    }
}
