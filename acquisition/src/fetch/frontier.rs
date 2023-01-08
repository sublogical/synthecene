use storelib::blob::{create_archive, write_multipart_file};
use calico_shared::result::{CalicoResult, CalicoError};
use std::path::Path;
use std::str::from_utf8;
use std::sync::Arc;
use prost::Message;
use futures::FutureExt;
use storelib::log::TransactionLog;
use tempfile::tempdir;
use uuid::Uuid;
use yaque::channel;
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;

use acquisition::protocol;

pub struct FrontierSender(yaque::Sender);

impl FrontierSender {
    pub async fn append_paths(&mut self, paths: &Vec<String>) -> CalicoResult<()> {
        let paths_bytes = paths.iter().map(|s| s.as_bytes().to_vec());
        
        self.0.send_batch(paths_bytes).await?;
        Ok(())
    }    
}
pub struct FrontierReceiver(yaque::Receiver);

impl Iterator for FrontierReceiver {
    type Item = CalicoResult<String>;

    /// Returns an iterator that always immediately returns.
    /// 
    /// None - indicates nothing is available
    /// Some(Ok(_)) - indicates a URL is available
    /// Some(Err(_)) - indicates something went wrong and you should freak out
    /// 
    fn next(&mut self) -> Option<Self::Item> {
        self.0.recv().now_or_never()
            .map(|result| match result {
                Ok(gaurd) => {
                    let output = from_utf8(&*gaurd).map(|url| url.to_string())?;
                    match gaurd.commit() {
                        Err(err) => {
                            return Err(CalicoError::from(err));
                        },
                        _ => {}
                    }
                    Ok(output)
                },
                Err(err) => {
                    Err(CalicoError::from(err))
                }
            })
    }
}

/// Frontier contains the queue(s) of prioritized & sorted URLs to be captured
pub struct FrontierStore {
    pub sender: FrontierSender,
    pub receiver: FrontierReceiver,
    pub path: String,
}

impl FrontierStore {
    pub fn init_local(path: &Path) -> FrontierStore {
        let (sender, receiver) = channel(path).unwrap();
        FrontierStore { 
            path: path.to_str().expect("Must always be utf8").to_string(),
            sender: FrontierSender(sender),
            receiver: FrontierReceiver(receiver)
         }
    }

    pub async fn checkpoint(&self, log: Arc<TransactionLog>, object_store: Arc<dyn ObjectStore>, base_path: &String) -> CalicoResult<()> {
        let temp = tempdir().unwrap();
        let token = Uuid::new_v4().to_string();
        let object_path = format!("{}/{}",base_path, token);
        let object_path: ObjectStorePath = object_path.try_into().unwrap();

        let temp_path = temp.path();

        create_archive(&self.path, &temp_path).await?;
    
        let (multipart_id, writer) = object_store.put_multipart(&object_path).await?;
        write_multipart_file(multipart_id, &writer, &temp_path).await?;
    
        // todo: create a commit in the transaction log pointing to this
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct LastVisitStore {
    store: kv::Store
}

impl LastVisitStore {
    pub fn init_local(path: &Path) -> CalicoResult<LastVisitStore> {
        let cfg = kv::Config::new(path);
        let store = kv::Store::new(cfg)?;

        Ok(LastVisitStore {
            store
        })
    }

    pub fn set(&mut self, path: &String, last_visit: protocol::LastVisit) -> CalicoResult<()>{
        let expected_len = last_visit.encoded_len();

        let mut buf = Vec::with_capacity(6);
        last_visit.encode(&mut buf)?;
        assert_eq!(expected_len, buf.len());

        let value = kv::Raw::from(buf);

        let bucket = self.store.bucket::<String, kv::Raw>(Some("last_visit"))?;
        bucket.set(&path, &value)?;

        Ok(())
    }

    pub fn contains(&mut self, path: &String) -> CalicoResult<bool> {
        let bucket = self.store.bucket::<String, kv::Raw>(Some("last_visit"))?;
        Ok(bucket.contains(path)?)
    }
}


#[cfg(test)]
mod tests {
    use std::{time::{Duration, Instant}, cmp::max};

    use crate::fetch::frontier::FrontierStore;

    use super::{LastVisitStore};
    use acquisition::protocol;
    use rand::{distributions::Alphanumeric, thread_rng, Rng};
    use tempfile::tempdir;
    use fs_extra::dir::get_size;

    #[tokio::test]
    async fn frontier_enqueue_dequeue_speedrun() {
        let temp = tempdir().unwrap();
        let mut frontier = FrontierStore::init_local(&temp.path());

        let start = Instant::now();
        for _ in 1..100_001 {
            let rand_string: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(60)
                .map(char::from)
                .collect();

            frontier.sender.append_paths(&vec![rand_string]).await.unwrap();
        }
        let folder_size = get_size(temp.path()).unwrap();
        assert!(start.elapsed() < Duration::from_secs(2));

        println!("Write URLs: {} ({:.2} kUrl/s)", 
            100_000, 
            100_000. / start.elapsed().as_millis() as f32);

        println!("Folder Size: {:.2} mb ({:.2} mb/s, {:.2} b/url)", 
            folder_size as f32 / 1_000_000., 
            folder_size as f32 / (start.elapsed().as_millis() as f32 * 1000.),
            folder_size as f32 / 100_000.
        );

        let start = Instant::now();
        let mut url = "".to_string();

        for res in frontier.receiver.into_iter() {
            url = match res {
                Ok(next_url) => max(url, next_url),
                Err(_) => panic!("nope")
            };
        }
        assert!(start.elapsed() < Duration::from_secs(2));

        println!("Read URLs: {} ({:.2} kUrl/s)", 
            100_000, 
            100_000. / start.elapsed().as_millis() as f32);

    }


    #[tokio::test]
    async fn last_visit_set_get_speedrun() {
        let temp = tempdir().unwrap();
        let mut store = LastVisitStore::init_local(&temp.path()).unwrap();
        let rand_string: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(50)
            .map(char::from)
            .collect();

        let start = Instant::now();
        for i in 1..100_001 {
            let last_visit = protocol::LastVisit {
                added_to_frontier: i,
                fetched: None,
                status_code: None
            };

            let path = format!("/{}/{:06}", rand_string, i);

            store.set(&path, last_visit).unwrap();
        }
        let folder_size = get_size(temp.path()).unwrap();
        assert!(start.elapsed() < Duration::from_secs(5));

        println!("Write URLs: {} ({:.2} kUrl/s)", 
            100_000, 
            100_000. / start.elapsed().as_millis() as f32);

        println!("Folder Size: {:.2} mb ({:.2} mb/s, {:.2} b/url)", 
            folder_size as f32 / 1_000_000., 
            folder_size as f32 / (start.elapsed().as_millis() as f32 * 1000.),
            folder_size as f32 / 100_000.
        );
    }
}


