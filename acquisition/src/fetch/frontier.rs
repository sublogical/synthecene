use calico_shared::types::systemtime_to_timestamp;
use storelib::blob::{create_archive, write_multipart_file, read_stream_file, open_archive};
use calico_shared::result::{CalicoResult, CalicoError};
use std::future;
use std::path::Path;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::SystemTime;
use prost::Message;
use futures::FutureExt;
use storelib::log::{TransactionLog, Commit, MAINLINE};
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

    log: Arc<TransactionLog>,
    object_store: Arc<dyn ObjectStore>,
    remote_path: String,
    local_path: String,
}

async fn restore_checkpoint(object_store: Arc<dyn ObjectStore>,
                            commit: &Commit<'_>,
                            local_path: &String) -> CalicoResult<()> {
    if commit.tile_files.len() != 1 {
        return Err(CalicoError::ImproperCheckpointCommit("Checkpoints should have exactly 1 tile".to_string()));
    }

    let tile_file = commit.tile_files.get(0).unwrap();
    if tile_file.file.len() != 1 {
        return Err(CalicoError::ImproperCheckpointCommit("Checkpoints should have exactly 1 file".to_string()));
    }

    let file = tile_file.file.get(0).unwrap();

    if file.file_type != storelib::protocol::FileType::Blob as i32 {
        return Err(CalicoError::ImproperCheckpointCommit("Checkpoints should always be type BLOB".to_string()));
    }

    let object_path: ObjectStorePath = file.file_path.clone().try_into().unwrap();

    // todo: shouldn't be assuming we can use tempdir for temporary storage here
    let temp = tempdir().unwrap();
    let download_archive_path = temp.path().join("download_archive.tgz");
    println!("download {} to {}", object_path, download_archive_path.display());

    // Download the archive file from storage
    let mut reader = object_store.get(&object_path).await?.into_stream();
    read_stream_file(&mut reader, &download_archive_path).await?;
    println!("open archive from {} to {}", download_archive_path.display(), local_path);

    open_archive(&download_archive_path, local_path).await?;

    Ok(())
}

impl FrontierStore {
    pub async fn init_local(object_store: Arc<dyn ObjectStore>,
                            remote_path: &String,
                            local_path: &String)
                            -> CalicoResult<FrontierStore> {
        // open or create transaction log
        let log = Arc::new(match TransactionLog::open(object_store.clone()).await {
            Ok(log) => Ok(log),
            Err(_) => TransactionLog::init(object_store.clone()).await
        }?);

        // if we can load a head commit from the transaction log, then there 
        // is an existing frontier checkpoint to work from
        if let Ok(commit) = log.head_mainline().await {
            println!("restoring checkpoint from {:?}", commit.commit_id);
            restore_checkpoint(object_store.clone(), &commit, &local_path).await?;
        }

        // todo: download existing frontier archive if appropriate
        println!("open frontier using {}", local_path);

        let (sender, receiver) = channel(local_path).unwrap();
        Ok(FrontierStore { 
            local_path: local_path.to_string(),
            remote_path: remote_path.to_string(),
            sender: FrontierSender(sender),
            receiver: FrontierReceiver(receiver).into_iter(),
            log,
            object_store: object_store.clone()
         })
    }

    pub async fn checkpoint(&mut self) -> CalicoResult<storelib::protocol::Commit> {
        let temp = tempdir().unwrap();
        let token = Uuid::new_v4().to_string();
        let object_path = format!("{}/{}",self.remote_path, token);
        let object_path: ObjectStorePath = object_path.try_into().unwrap();

        // todo: shouldn't be assuming we can use tempdir for temporary storage here
        let temp_path = temp.path().join("frontier_archive.tgz");
        let timestamp = systemtime_to_timestamp(SystemTime::now());

        self.receiver.0.save()?;

        create_archive(&self.local_path, &temp_path).await?;
    
        let (multipart_id, mut writer) = self.object_store.put_multipart(&object_path).await?;
        let bytes_written = write_multipart_file(multipart_id, &mut writer, &temp_path).await?;

        let tile_file = storelib::protocol::TileFiles {
            tile: None, // for now, the frontier doesn't use tiles
            file: vec![storelib::protocol::File {
                file_path: object_path.to_string(),
                file_type: storelib::protocol::FileType::Blob as i32,
                file_size: bytes_written,
                update_time: timestamp,
            }],
        };

        let head_id = self.log.head_id_mainline().await?;
        let commit = self.log.create_commit(&head_id,
            Some(calico_shared::applications::CRAWLER.to_string()),
            Some(calico_shared::applications::CRAWLER.to_string()),
            Some("Frontier Checkpoint".to_string()),
            timestamp,
            vec![],
            vec![],
            vec![tile_file]).await?;

        let _new_head = self.log.fast_forward(MAINLINE, &commit.commit_id).await.unwrap();

        Ok(commit.commit)
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
    use std::{time::{Duration, Instant}, cmp::max, sync::Arc};

    use crate::fetch::frontier::FrontierStore;

    use super::{LastVisitStore};
    use acquisition::protocol;
    use object_store::{local::LocalFileSystem, ObjectStore};
    use rand::{distributions::Alphanumeric, thread_rng, Rng};
    use tempfile::tempdir;
    use fs_extra::dir::get_size;

    async fn populate_frontier(frontier: &mut FrontierStore, num:usize) {
        for _ in 1..num+1 {
            let rand_string: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(60)
                .map(char::from)
                .collect();

            frontier.sender.append_paths(&vec![rand_string]).await.unwrap();
        }
    }

    async fn drain_frontier(frontier: &mut FrontierStore) -> usize {
        let mut url = "".to_string();
        let mut count = 0;

        for res in (&mut frontier.receiver).into_iter() {
            url = match res {
                Ok(next_url) => {
                    count += 1;
                    max(url, next_url)
                },
                Err(_) => panic!("nope")
            };
        }
        count
    }
    
    #[tokio::test]
    async fn frontier_enqueue_dequeue_speedrun() {
        let temp = tempdir().unwrap();
        let temp_logdir = tempdir().unwrap();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(temp_logdir.path()).unwrap());

        let mut frontier = FrontierStore::init_local(object_store,
            &"frontier".to_string(),
            &temp.path().to_str().unwrap().to_string()).await.unwrap();

        let start = Instant::now();
        populate_frontier(&mut frontier, 100_000).await;
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
        let read_urls = drain_frontier(&mut frontier).await;
        assert!(start.elapsed() < Duration::from_secs(2));

        println!("Read URLs: {} ({:.2} kUrl/s)", 
            read_urls, 
            read_urls as f32 / start.elapsed().as_millis() as f32);

    }

    #[tokio::test]
    async fn frontier_checkpoint() {
        let frontier_local_1 = tempdir().unwrap();
        let temp_logdir = tempdir().unwrap();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(temp_logdir.path()).unwrap());

        let mut frontier_1 = FrontierStore::init_local(object_store.clone(),
            &"frontier".to_string(),
            &frontier_local_1.path().to_str().unwrap().to_string()).await.unwrap();

        populate_frontier(&mut frontier_1, 1_000).await;

        let commit = frontier_1.checkpoint().await.unwrap();
        println!("stored checkpoint in {:?}", commit.commit_id);
        let frontier_local_size_1 = get_size(frontier_local_1.path()).unwrap();

        let frontier_local_2 = tempdir().unwrap();
        assert_ne!(frontier_local_1.path(), frontier_local_2.path());

        let mut frontier_2 = FrontierStore::init_local(object_store.clone(),
            &"frontier".to_string(),
            &frontier_local_2.path().to_str().unwrap().to_string()).await.unwrap();
        let frontier_local_size_2 = get_size(frontier_local_2.path()).unwrap();
        assert_eq!(frontier_local_size_1, frontier_local_size_2);

        let read_urls = drain_frontier(&mut frontier_2).await;
        assert_eq!(1_000, read_urls);
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


