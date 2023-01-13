use calico_shared::types::systemtime_to_timestamp;
use prost::bytes::{Buf, Bytes};
use rand::RngCore;
use storelib::blob::{create_archive, write_multipart_file, read_stream_file, open_archive};
use std::fmt::Debug;
use std::io;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use result::ResultOptionExt;
use storelib::log::{TransactionLog, Commit, MAINLINE};
use tempfile::tempdir;
use uuid::Uuid;
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;

use acquisition::protocol;

use super::{full_url, retrieve};

// This generates an efficient but lossy key:
// - no guarantee of ordering in a distribute system
// - no guarantee of collisions (though unlikely to occur before sun explodes)
//
// Key Structure
// [0..4]     Priority  [u8;1]
// [4..100]   Nano time [u8;12]
// [100..128] Random    [u8;3]
fn generate_lossy_key(priority: u8) -> [u8;16] {
    // take only the bottom 96 bits of epoch time in nanos. This should be valid for 2.5123086e+12 years
    let mask:u128 = (1 << 96) - 1;
    let id:u128 = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() & mask).try_into().unwrap();    
    let mut pad = [0u8;3];
    rand::thread_rng().fill_bytes(&mut pad);

    let mut key = [0u8;16];
    
    key[..1].clone_from_slice(&priority.to_be_bytes());
    key[1..13].clone_from_slice(&id.to_be_bytes()[4..16]);
    key[13..16].clone_from_slice(&pad);
    key
}

#[derive(Debug)]
pub enum Error {
    ImproperCheckpointCommit(String),
    FailedCommit(Box<dyn std::error::Error>),
    CommitCollision(Box<dyn std::error::Error>),

    IoError(io::Error),
    DecodeError(prost::DecodeError),
    EncodeError(prost::EncodeError),
    DatabaseError(rocksdb::Error),
    ObjectStoreError(object_store::Error)
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::IoError(err)
    }

}

impl From<rocksdb::Error> for Error {
    fn from(err: rocksdb::Error) -> Self {
        Error::DatabaseError(err)
    }

}
impl From<object_store::Error> for Error {
    fn from(err: object_store::Error) -> Self {
        Error::ObjectStoreError(err)
    }
}    

impl From<prost::EncodeError> for Error {
    fn from(err: prost::EncodeError) -> Self {
        Error::EncodeError(err)
    }
}    

impl From<prost::DecodeError> for Error {
    fn from(err: prost::DecodeError) -> Self {
        Error::DecodeError(err)
    }
}    

pub type Result<T> = std::result::Result<T, Error>;


#[derive(Debug)]
pub struct ItemGuard<T>{
    db: Arc<rocksdb::DB>,
    cf: String,
    key: Box<[u8]>,
    value: T,
    save: bool
}

impl <T> ItemGuard<T> {
    /// Commits the changes to the queue
    pub fn save(mut self) {
        self.save = true;
    }
}

impl <T> Drop for ItemGuard<T> {
    fn drop(&mut self) {
        if !self.save {
            let cf = self.db.cf_handle(&self.cf).expect("incorrect cf name");
            self.db.delete_cf(&cf, &self.key).expect("should be able to delete the record we just read");
        }
    }
}

impl <T> Deref for ItemGuard<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl <T> DerefMut for ItemGuard<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

pub struct PriorityQueue(Arc<rocksdb::DB>, String);

impl PriorityQueue {
    pub fn init(db: Arc<rocksdb::DB>, cf: &str) -> Result<PriorityQueue> 
    {
        let cf = cf.to_string();

        Ok(PriorityQueue(db, cf))
    }
    
    fn cf(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.0.cf_handle(&self.1).expect("incorrect cf name")
    }

    /**
     * appends raw bytes to the queue
     */
    pub fn append<B>(&self, priority: u8, value: B) -> Result<()> 
    where
        B: AsRef<[u8]>
    {
        let key = generate_lossy_key(priority);

        self.0.put_cf(&self.cf(), key, &value)?;

        Ok(())
    }

    /**
     * Append a message to the queue, serializing it with prost
     */
    pub fn append_message<T>(&self, priority: u8, message: &T) -> Result<()>
    where
        T : prost::Message 
    {
        let expected_len = message.encoded_len();

        let mut buf = Vec::with_capacity(18);
        message.encode(&mut buf)?;
        assert_eq!(expected_len, buf.len());

        self.append(priority, &buf)
    }

    /**
     * Iterate over the queue, returning raw bytes
     *
     * Note that this iterator will destroy the entries as it iterates over them. If you wish to save an entry, call `save` on the `ItemGuard` before dropping it.
     */
    pub fn iter<'db : 'iter, 'iter>(&'db mut self) -> PriorityQueueIterator<'iter> {
        let iter = self.0.iterator_cf(&self.cf(), rocksdb::IteratorMode::Start); // Always iterates forward
        PriorityQueueIterator { iter, db: self.0.clone(), cf: self.1.clone() }
    }

    /**
     * Iterate over the queue, decoding the messages with prost
     * 
     * Note that this iterator will destroy the messages as it iterates over them. If you wish to save a message, call `save` on the `ItemGuard` before dropping it.
     */
    pub fn iter_message<'db : 'iter, 'iter, T>(&'db mut self) -> PriorityQueueMessageIterator<'iter,T> 
    where
        T : prost::Message + Default
    {
        let iter = self.0.iterator_cf(&self.cf(), rocksdb::IteratorMode::Start); // Always iterates forward
        let inner = PriorityQueueIterator { iter, db: self.0.clone(), cf: self.1.clone() };
        PriorityQueueMessageIterator(inner, PhantomData)
    }

}

pub struct PriorityQueueMessageIterator<'iter, T: prost::Message>(PriorityQueueIterator<'iter>, PhantomData<T>);
impl <'iter, T : prost::Message + Default> Iterator for PriorityQueueMessageIterator<'iter, T> {
    type Item = Result<ItemGuard<T>> ;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
            .map(|res|
                res.and_then(|item| {
                    let db = item.db.clone();
                    let cf = item.cf.clone();
                    let key = item.key.clone();

                    let out = T::decode(item.as_ref())                    
                        .map_err(|err| Error::DecodeError(err))
                        .map(|value| ItemGuard { db, cf, key, value, save: false });

                    item.save();

                    out
                }))
    }
}

pub struct PriorityQueueIterator<'iter> {
    iter: rocksdb::DBIterator<'iter>,
    db: Arc<rocksdb::DB>,
    cf: String,
}

impl <'iter> Iterator for PriorityQueueIterator<'iter> {
    type Item = Result<ItemGuard<Box<[u8]>>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
            .map(|result| result
                .map_err(|err| Error::DatabaseError(err))
                .map(|(key, value)| 
                    ItemGuard { db: self.db.clone(), cf: self.cf.clone(), key, value, save: false }))
    }
}


pub struct KvTable(Arc<rocksdb::DB>, String);

impl KvTable {
    pub fn init(db: Arc<rocksdb::DB>, cf: &str) -> Result<KvTable> 
    {
        let cf = cf.to_string();

        Ok(KvTable(db, cf))
    }
    
    /**
     * Get the column family handle for this table
     */
    fn cf(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.0.cf_handle(&self.1).expect("incorrect cf name")
    }

    /**
     * Get a value from the table returning raw bytes
     */
    pub fn get<K,V>(&self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>
    {
        Ok(self.0.get_cf(&self.cf(), key)?)
    }

    /**
     * Get a message from the table, decoding it with prost
     */
    pub fn get_message<K,T>(&self, key: K) -> Result<Option<T>>
    where
        K: AsRef<[u8]>,
        T : prost::Message + Default
    {
        let value = self.0.get_cf(&self.cf(), key)?;

        let res = value.map(|bytes| T::decode(Bytes::from(bytes)));

        // convert Option<Result> to Result<Option>, mapping the error to ours
        Ok(res.invert()?)
    }

    /**
     * Check if a key exists in the table
     */
    pub fn contains<K>(&self, key: K) -> Result<bool>
    where
        K: AsRef<[u8]>
    {
        Ok(self.0.get_cf(&self.cf(), key)?.is_some())
    }

    /**
     * Put a value into the table, using raw bytes
     */
    pub fn put<K, B>(&self, key: K, value: B) -> Result<()> 
    where
        K: AsRef<[u8]>,
        B: AsRef<[u8]>
    {
        self.0.put_cf(&self.cf(), key, &value)?;

        Ok(())
    }

    /**
     * Put a message into the table, encoding it with prost
     */
    pub fn put_message<K,T>(&self, key: K, message: &T) -> Result<()>
    where
        K: AsRef<[u8]>,
        T : prost::Message 
    {
        let expected_len = message.encoded_len();

        let mut buf = Vec::with_capacity(18);
        message.encode(&mut buf)?;
        assert_eq!(expected_len, buf.len());

        self.put(key, &buf)
    }
}

pub struct DomainState {
    pub local_path: PathBuf,
    pub remote_path: PathBuf,
    pub db: Arc<rocksdb::DB>,
    pub frontier: PriorityQueue,
    pub last_visit: KvTable,
    pub host: protocol::Host,
    pub robots: Option<String>,
}

impl DomainState {
    pub(crate) async fn fetch_robots(host: &protocol::Host) -> Result<Option<String>> {
        let url = full_url(host, &"/robots.txt".to_string());
        let robots = retrieve(url).await.map(|robots_capture| robots_capture.body).ok();

        Ok(robots)
    }

    pub(crate) fn open_db<I>(path:I) -> Result<Arc<rocksdb::DB>>
    where
        I: AsRef<Path>
    {
        let mut cf_opts = rocksdb::Options::default();
        cf_opts.set_max_write_buffer_number(16);
        let cf_frontier = rocksdb::ColumnFamilyDescriptor::new("cf_frontier", cf_opts);

        let mut cf_opts = rocksdb::Options::default();
        cf_opts.set_max_write_buffer_number(16);
        let cf_last_visit = rocksdb::ColumnFamilyDescriptor::new("cf_last_visit", cf_opts);

        let mut db_opts = rocksdb::Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        Ok(Arc::new(rocksdb::DB::open_cf_descriptors(&db_opts, path, vec![cf_frontier, cf_last_visit])?))
    }

    pub(crate) async fn download_checkpoint<O>(object_store: Arc<dyn ObjectStore>,
                                               commit: &Commit<'_>,
                                               output_path:O) -> Result<()>
    where
        O: AsRef<Path> + Debug
    {
        if commit.tile_files.len() != 1 {
            return Err(Error::ImproperCheckpointCommit("Checkpoints should have exactly 1 tile".to_string()));
        }
    
        let tile_file = commit.tile_files.get(0).unwrap();
        if tile_file.file.len() != 1 {
            return Err(Error::ImproperCheckpointCommit("Checkpoints should have exactly 1 file".to_string()));
        }
    
        let file = tile_file.file.get(0).unwrap();
    
        if file.file_type != storelib::protocol::FileType::Blob as i32 {
            return Err(Error::ImproperCheckpointCommit("Checkpoints should always be type BLOB".to_string()));
        }
    
        let object_path: ObjectStorePath = file.file_path.clone().try_into().unwrap();
    
        println!("download {} to {:?}", object_path, output_path);

        // Download the archive file from storage
        let mut reader = object_store.get(&object_path).await?.into_stream();
        read_stream_file(&mut reader, &output_path).await?;    
        
        todo!("re-implement open checkpoint logic");        
    }
    /*
        // todo: shouldn't be assuming we can use tempdir for temporary storage here
        let temp = tempdir().unwrap();
        let download_archive_path = temp.path().join("download_archive.tgz");
 */

    pub(crate) async fn open_checkpoint<I, O>(input_path:I, output_path:I) -> Result<()>
    where
        O: AsRef<Path> + Debug,
        I: AsRef<Path> + Debug
    {
        open_archive(input_path, output_path).await?;

        todo!("re-implement open checkpoint logic");        
    }

    pub async fn init<I>(host: &protocol::Host, 
                         local_path:I,
                         remote_path:I) -> Result<DomainState>
    where
        I: AsRef<Path>
    {
        let local_path = local_path.as_ref().to_path_buf();
        let remote_path = remote_path.as_ref().to_path_buf();

        let robots = Self::fetch_robots(host).await?;
        let db = Self::open_db(local_path.clone())?;
        let frontier = PriorityQueue::init(db.clone(), "cf_frontier")?;
        let last_visit = KvTable::init(db.clone(), "cf_last_visit")?;
        let host = host.clone();

        Ok(DomainState { local_path, remote_path, host, robots, db, frontier, last_visit })
    }

    pub async fn create_checkpoint<O>(&mut self, output_path:O) -> Result<()>
    where
        O: AsRef<Path> + Debug
    {
        self.db.flush()?;

        create_archive(&self.local_path, &output_path).await?;
    
        Ok(())
    }

    /**
     * Get a path to a remote checkpoint file
     */
    fn get_remote_checkpoint_path(&self) -> PathBuf 
    {
        let token = Uuid::new_v4().to_string();
        let mut remote_path = self.remote_path.clone();
        remote_path.push(format!("checkpoint-{}.tgz", token));
        remote_path
    }

    /**
     * Upload a checkpoint to remote storage
     * 
     * * `object_store` - The object store to upload to
     * * `input_path` - The path to the checkpoint file to upload
     * * `output_path` - The remote path to the checkpoint file to upload
     */
    pub(crate) async fn upload_checkpoint<I,O>(object_store: &Arc<dyn ObjectStore>,
                                               input_path:I, 
                                               output_path:O) -> Result<u64>
    where
        O: AsRef<Path> + Debug,
        I: AsRef<Path> + Debug
    {
        let input_path = input_path.as_ref().to_string_lossy().to_string();
        let object_path: object_store::path::Path = input_path.try_into().unwrap();

        let (multipart_id, mut writer) = object_store.put_multipart(&object_path).await?;
        let bytes_written = write_multipart_file(multipart_id, &mut writer, output_path).await?;

        Ok(bytes_written)
    }

    pub(crate) async fn commit_checkpoint<O>(log: Arc<TransactionLog>,
                                             output_path:O,
                                             bytes_written: u64) -> Result<storelib::protocol::Commit> 
    where
        O: AsRef<Path> + Debug
    {  
        let timestamp = systemtime_to_timestamp(SystemTime::now());
   
        let tile_file = storelib::protocol::TileFiles {
            tile: None, // for now, the frontier doesn't use tiles
            file: vec![storelib::protocol::File {
                file_path: output_path.as_ref().to_string_lossy().to_string(),
                file_type: storelib::protocol::FileType::Blob as i32,
                file_size: bytes_written,
                update_time: timestamp,
            }],
        };

        let head_id = log.head_id_mainline().await
            .map_err(|err| Error::FailedCommit(Box::new(err)))?;

        let commit = log.create_commit(&head_id,
            Some(calico_shared::applications::CRAWLER.to_string()),
            Some(calico_shared::applications::CRAWLER.to_string()),
            Some("Frontier Checkpoint".to_string()),
            timestamp,
            vec![],
            vec![],
            vec![tile_file]).await
            .map_err(|err| Error::FailedCommit(Box::new(err)))?;

        let _new_head = log.fast_forward(MAINLINE, &commit.commit_id).await
            .map_err(|err| Error::CommitCollision(Box::new(err)))?;


        Ok(commit.commit)
    }

    pub async fn checkpoint(&mut self,
                            object_store: Arc<dyn ObjectStore>,
                            log: Arc<TransactionLog>) -> Result<storelib::protocol::Commit> {
        // todo: shouldn't be assuming we can use tempdir for temporary storage here
        let temp = tempdir().unwrap();
        let temp_path = temp.path().join("frontier_archive.tgz");

        let remote_path = self.get_remote_checkpoint_path();
        self.create_checkpoint(&temp_path).await?;

        let bytes_written = Self::upload_checkpoint(&object_store, &temp_path, &remote_path).await?;

        let commit = Self::commit_checkpoint(log, &remote_path, bytes_written).await?;

        Ok(commit)
    }
}


#[cfg(test)]
mod tests {
    use std::{time::{Duration, Instant, SystemTime, UNIX_EPOCH}, cmp::max, sync::Arc};

    use crate::fetch::state::{generate_lossy_key, DomainState};

    use super::*;
    use acquisition::protocol;
    use rand::{distributions::Alphanumeric, thread_rng, Rng};
    use tempfile::tempdir;
    use fs_extra::dir::get_size;

    async fn populate_frontier(frontier: &mut PriorityQueue, num:usize) {
        for _ in 1..num+1 {
            let rand_string: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(60)
                .map(char::from)
                .collect();
            frontier.append(0, rand_string.as_bytes()).unwrap();
        }
    }

    async fn drain_frontier(frontier: &mut PriorityQueue) -> usize {
        let mut url = "".to_string();
        let mut count = 0;

        for item in frontier.iter() {
            match item {
                Ok(item) => {
                    let next_url = std::str::from_utf8(&item).unwrap().to_string();
                    count += 1;
                    url = max(url, next_url);
                },
                Err(_) => todo!(),
            }
        }
        count
    }

    #[test]
    fn test_lossy_key() {
        let k1 = generate_lossy_key(2);
        let k2 = generate_lossy_key(2); // assumes it takes > 1ns to make the two calls
        let k3 = generate_lossy_key(1);
        assert!(k1<k2);
        assert!(k3<k1);
    }

    #[tokio::test]
    async fn priority_enqueue_dequeue_speedrun() {
        let temp = tempdir().unwrap();

        let db = DomainState::open_db(temp.path()).unwrap();
        let mut frontier = PriorityQueue::init(db.clone(), "cf_frontier").unwrap();

        let num_urls = 100_000;

        let start = Instant::now();
        populate_frontier(&mut frontier, num_urls).await;
        let folder_size = get_size(temp.path()).unwrap();

        println!("Write URLs: {} ({:.2} kUrl/s)", 
            num_urls, 
            num_urls as f32 / start.elapsed().as_millis() as f32);
        assert!(num_urls as f32 / start.elapsed().as_millis() as f32 > 25.0); // single thread write rate 25k TPS+

        println!("Folder Size: {:.2} mb ({:.2} mb/s, {:.2} b/url)", 
            folder_size as f32 / 1_000_000., 
            folder_size as f32 / (start.elapsed().as_millis() as f32 * 1000.),
            folder_size as f32 / 100_000.
        );

        let start = Instant::now();
        let read_urls = drain_frontier(&mut frontier).await;

        println!("Read URLs: {} ({:.2} kUrl/s)", 
            read_urls, 
            read_urls as f32 / start.elapsed().as_millis() as f32);
        assert!(read_urls as f32 / start.elapsed().as_millis() as f32 > 40.0);// single thread write rate 40k TPS+

    }

    /**
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
 **/

    #[tokio::test]
    async fn last_visit_set_get_speedrun() {
        let temp = tempdir().unwrap();
        let db = DomainState::open_db(temp.path()).unwrap();
        let mut store = KvTable::init(db.clone(), "cf_last_visit").unwrap();

        let rand_string: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(50)
            .map(char::from)
            .collect();

        let start = Instant::now();
        for i in 1..100_001 {
            let last_visit = protocol::LastVisit {
                added_to_frontier: true,
                ..Default::default()
            };

            let path = format!("/{}/{:06}", rand_string, i);

            store.put_message(&path, &last_visit).unwrap();
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


