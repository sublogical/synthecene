use bytes::Bytes;
use futures::{TryFutureExt, TryStreamExt, Future};
use itertools::{sorted, Itertools};
use log::info;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use prost::Message;
use rand::Rng;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use crate::log_protocol;
use crate::partition::ColumnRef;
use crate::result::{CalicoResult, CalicoError};

type BranchRef<'a> = &'a str;
pub const MAINLINE: &str = "mainline";

pub struct LogConfig<'a> {
    log_path: &'a Path
}

pub struct TableView<'a> {
    checkpoint: Option<Checkpoint<'a>>,
    history: Vec<Commit<'a>>,
}

impl TableView<'_> {
    pub fn has_checkpoint(&self) -> bool {
        self.checkpoint.is_some()
    }
    
    async fn from_history<'a>(log: &'a TransactionLog, history: Vec<Commit<'a>>) -> CalicoResult<TableView<'a>> {
        let mut since_checkpoint:Vec<Commit> = Vec::new();

        for commit in history {
            if commit.has_checkpoint().await? {
                let cp:Checkpoint = log.checkpoint(&commit.commit_id).await?.clone();

                return Ok(TableView {
                    checkpoint: Some(cp),
                    history: since_checkpoint
                });

            } else {
                since_checkpoint.push(commit.clone());
            }
        }

        // no checkpoint found, return what we have
        // todo: should we warn if we didn't hit end of history?
        return Ok(TableView {
            checkpoint: None,
            history: since_checkpoint
        });
    }
}



#[derive(Clone, Debug)]
pub struct Commit<'a>{
    commit: log_protocol::Commit,
    log: &'a TransactionLog
}

impl Deref for Commit<'_> {
    type Target = log_protocol::Commit;
    fn deref(&self) -> &log_protocol::Commit { &self.commit }
}

impl Commit<'_> {
    async fn history(&self, max_history: u32) -> CalicoResult<Vec<Commit>> {
        let mut max_history = max_history;
        let mut history:Vec<Commit> = Vec::new();   
        let mut curr = self;
        let mut next_id = curr.parent_id.to_vec();

        println!("{}: reading commit history", hex::encode(&self.commit_id));

        while max_history > 0 {
            history.push(curr.clone());

            println!("{}: reading {}", hex::encode(&self.commit_id), hex::encode(&next_id));
            match self.log.get_commit(&next_id).await {
                Ok(commit) => {
                    next_id = commit.parent_id.to_vec();
                }
                Err(e) => {
                    println!("{}: NOT FOUND {}", hex::encode(&self.commit_id), hex::encode(&next_id));
                    return Ok(history);
                }
            }
            max_history -= 1;
        }

        Ok(history)
    }

    async fn view(&self, max_history: u32) -> CalicoResult<TableView> {
        let history = self.history(max_history).await?;
        TableView::from_history(self.log, history).await
    }

    // Determines whether there is an existing checkpoint for this commit
    async fn has_checkpoint(&self) -> CalicoResult<bool> {
        self.log.has_checkpoint(&self.commit_id).await
    }

    async fn checkpoint(&self) -> CalicoResult<Checkpoint> {
        self.log.checkpoint(&self.commit_id).await
    }

    // walks the default parent heirarchy back until it finds the commit that 
    // represents the state of this lineage at the specified timestamp, ie the
    // first commit found at a timestamp equal to or less than <timestamp>
    pub async fn at_timestamp(&self, timestamp: u64) -> CalicoResult<Commit> {
        let mut candidate_commit = self;
        let mut candidate_timestamp = self.timestamp;

        // todo: consider freaking out if the history is branching

        /*
        while timestamp > candidate_timestamp {
            candidate_commit = &self.log.get_commit(&candidate_commit.parent_id).await?;
            candidate_timestamp = candidate_commit.timestamp;
        }
         */
//        Ok(candidate_commit.clone())
        todo!("FINISH");
    }

}

#[derive(Clone, Debug)]
pub struct Checkpoint<'a> {
    checkpoint: log_protocol::Checkpoint,
    log: &'a TransactionLog
}

impl Deref for Checkpoint<'_> {
    type Target = log_protocol::Checkpoint;
    fn deref(&self) -> &log_protocol::Checkpoint { &self.checkpoint }
}

#[derive(Clone, Debug)]
pub struct TransactionLog {
    object_store: Arc<Box<dyn ObjectStore>>
}

impl TransactionLog {
    pub async fn init<'a>(log_config: &LogConfig<'a>) -> CalicoResult<TransactionLog> {
        let log = TransactionLog{ 
            object_store: Arc::new(Self::get_object_store(log_config)?)
        };

        log.init_branch(MAINLINE).await?;

        Ok(log)
    }

    fn get_object_store(log_config: &LogConfig) -> CalicoResult<Box<dyn ObjectStore>> {
        Ok(Box::new(LocalFileSystem::new_with_prefix(log_config.log_path)?))
    }
    
    pub fn open(log_path: &str) -> CalicoResult<TransactionLog> {
        todo!("check to see if the path exists and is initialized, panic if not");
    }

    pub async fn head<'a>(&self, branch: BranchRef<'a>) -> CalicoResult<Commit> {
        let commit_id = self.head_id(branch).await?;

        println!("HEAD ID = {:?}", commit_id);
        let commit = self.get_commit(&commit_id).await?;

        Ok(commit)
    }

    pub async fn head_mainline(&self) -> CalicoResult<Commit> {
        self.head(MAINLINE).await
    }

    pub async fn head_id<'a>(&self, branch: BranchRef<'a>) -> CalicoResult<Vec<u8>> 
    {
        let mut prot_ref = self.get_last_ref(branch).await?;

        Ok(prot_ref.commit_id)
    }

    pub async fn head_id_mainline(&self) -> CalicoResult<Vec<u8>> {
        self.head_id(MAINLINE).await
    }

    pub async fn init_branch<'a>(&self, branch: BranchRef<'a>) -> CalicoResult<log_protocol::Ref> {
        let mut prot_ref = log_protocol::Ref::default();

        prot_ref.label = branch.to_string();
        prot_ref.ref_seq = 0;
        prot_ref.commit_id = [0;20].to_vec();

        self.put_ref(branch, prot_ref).await
    }


    pub async fn get_commit(&self, commit_id: &Vec<u8>) -> CalicoResult<Commit> {
        let path: ObjectStorePath = format!("commit/{}", hex::encode(&commit_id)).try_into().unwrap();

        // todo: support a local in-memory object store cache

        info!("{}: reading commit record", hex::encode(&commit_id));
        let result = self.object_store.get(&path).await?;
        let bytes = result.bytes().await?;
        let commit:log_protocol::Commit = log_protocol::Commit::decode(bytes)?;

        Ok(Commit {
            commit,
            log: &self
        })
    }

    // Creates and commits a single commit to the transaction log. Note that 
    // this will not advance the head reference, so this commit must be 
    // separately merged into a branch

    pub async fn create_commit(&self, 
                         parent_id: &Vec<u8>,
                         _application: Option<String>,
                         _committer: Option<String>,
                         _commit_message: Option<String>,
                         commit_timestamp: u64,
                         columns: Vec<ColumnRef>,
                         _column_expressions: Vec<(ColumnRef, String)>,
                         files: Vec<log_protocol::File>) -> CalicoResult<Commit> {
        
        let mut commit = log_protocol::Commit::default();

        // todo: any way to avoid vec<u8> for this since we have fixed size?
        commit.commit_id = rand::thread_rng().gen::<[u8; 20]>().to_vec();
    
        commit.parent_id = parent_id.to_vec();
        commit.timestamp = commit_timestamp;
        commit.columns = columns;
        commit.file = files;

        let expected_len = commit.encoded_len();

        let mut buf = Vec::with_capacity(18);
        commit.encode(&mut buf)?;
        assert_eq!(expected_len, buf.len());

        let path: ObjectStorePath = format!("commit/{}", hex::encode(&commit.commit_id)).try_into().unwrap();
        let data = Bytes::from(buf);
        
        println!("{}: storing commit record", hex::encode(&commit.commit_id));
        self.object_store.put(&path, data).await?;

        Ok(Commit {
            commit,
            log: &self
        })
    }

    // finds the last instance of a branch ref
    async fn find_last_ref<'a>(&self, branch: BranchRef<'a>) -> CalicoResult<ObjectStorePath> {
        let prefix: ObjectStorePath = format!("refs/{}/", branch).try_into().unwrap();

        let mut paths = self.object_store.list(Some(&prefix))
            .await?
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<ObjectStorePath>>().await?;
        
        paths.sort();
        paths.last()
            .map(|path| path.clone())
            .ok_or(CalicoError::BranchNotFound(branch.to_string()))
    }

    async fn get_last_ref<'a>(&self, branch: BranchRef<'a>) -> CalicoResult<log_protocol::Ref> {
        let ref_path = self.find_last_ref(branch).await?;
        println!("Last Ref: {} = {}", branch, ref_path);

        // todo: support a local in-memory object store cache

        info!("{}: reading branch ref {}", branch, ref_path);

        let result = self.object_store.get(&ref_path).await?;
        let bytes = result.bytes().await?;
        let prot_ref:log_protocol::Ref = log_protocol::Ref::decode(bytes)?;

        Ok(prot_ref)
    }

    async fn put_ref<'a>(&self, branch: BranchRef<'a>, prot_ref: log_protocol::Ref) -> CalicoResult<log_protocol::Ref> {
        let expected_len = prot_ref.encoded_len();

        let mut buf = Vec::with_capacity(18);
        prot_ref.encode(&mut buf)?;
        assert_eq!(expected_len, buf.len());

        let path: ObjectStorePath = format!("refs/{}/{:08x}", branch, prot_ref.ref_seq).try_into().unwrap();
        let data = Bytes::from(buf);

        info!("{}: writing branch ref {}", branch, path);
        self.object_store.put(&path, data).await?;

        Ok(prot_ref)
    }

    // Perform a fast-forward only merge on a branch to a commit.
    pub async fn fast_forward<'a>(&self,
                                  branch: BranchRef<'a>,
                                  commit_id: &Vec<u8>) -> CalicoResult<log_protocol::Ref> {
        let mut prot_ref = self.get_last_ref(branch).await?;

        // TODO: make sure we're actually fast forwarding!

        prot_ref.ref_seq += 1;
        prot_ref.commit_id = commit_id.to_vec();

        self.put_ref(branch, prot_ref).await
    }

    // Creates a new checkpoint based on the specified commit using the set of objects referenced for the snapshot
    pub fn create_checkpoint(&self, 
                             commit_id: &Vec<u8>,
                             files: &Vec<log_protocol::File>) -> CalicoResult<Checkpoint> {
            
        Ok(Checkpoint {
            log: self,
            checkpoint: log_protocol::Checkpoint::default(),
        })
    }

    // Determines whether there is an existing checkpoint for the passed in commit
    async fn has_checkpoint(&self, commit_id: &Vec<u8>) -> CalicoResult<bool> {
        Ok(false)
    }

    async fn checkpoint(&self, commit_id: &Vec<u8>) -> CalicoResult<Checkpoint> {
        todo!("load the actual checkpoint");
    }

}


#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use std::{fs, time};

    use crate::log::{TransactionLog, LogConfig, Commit };
    use crate::log_protocol;
    use crate::result::CalicoResult;
    use crate::result::CalicoError::ObjectStoreError;
    use std::io::ErrorKind::NotFound;

    use super::MAINLINE;

    #[tokio::test]
    async fn create_new_transaction_log() -> CalicoResult<()> {
        let temp_logdir = tempdir().unwrap();

        let log_config = LogConfig {
            log_path: temp_logdir.path()
        };

        let log = TransactionLog::init(&log_config).await?;
        let head_result = log.head_mainline().await;
        assert!(head_result.is_err());

        Ok(())
    }

    async fn test_commit_push<'a>(log:&'a TransactionLog, timestamp: u64, filename: &str) -> Commit<'a> {
        let cols = vec!["a".to_string()];
        let col_expr = vec![("a".to_string(), "$new".to_string())];
        let head_id = log.head_id_mainline().await.unwrap();
        let file = log_protocol::File {
            file_path: filename.to_string(),
            file_type: log_protocol::FileType::Data as i32,
            file_size: 1,
            update_time: timestamp
        };

        let commit = log.create_commit(
            &head_id.to_vec(), 
            None, 
            None, 
            None, 
            timestamp, 
            cols, 
            col_expr, 
            vec![file]).await.unwrap();

        let new_head = log.fast_forward(MAINLINE, &commit.commit_id).await.unwrap();

        commit
    }

    #[tokio::test]
    async fn round_trip() -> CalicoResult<()> {
        let temp_logdir = tempdir().unwrap();

        let log_config = LogConfig {
            log_path: temp_logdir.path()
        };

        let log = TransactionLog::init(&log_config).await?;
        let commit = test_commit_push(&log, 1, "a").await;
        let commit_dir = log_config.log_path.join("commit");
        let commit_file = fs::read_dir(commit_dir).unwrap().nth(0).unwrap().unwrap();
        let commit_size = fs::metadata(commit_file.path()).unwrap().len();

        assert!(commit_size > 0);

        let read_commit = log.get_commit(&commit.commit_id).await?;

        assert_eq!(read_commit.timestamp, 1);
        assert_eq!(read_commit.file[0].file_path, "a");

        Ok(())
    }



    #[tokio::test]
    async fn appends_in_log() -> CalicoResult<()> {

        let temp_logdir = tempdir().unwrap();

        let log_config = LogConfig {
            log_path: temp_logdir.path()
        };

        let log = TransactionLog::init(&log_config).await?;
        test_commit_push(&log, 1, "a").await;

        let head = log.head_mainline().await?;
        let history = head.history(100).await?;
        assert_eq!(history.len(), 1);

        let table = head.view(100).await?;
        assert!(!table.has_checkpoint());
        assert_eq!(table.history.len(), 1);

        test_commit_push(&log, 1, "b").await;

        let head = log.head_mainline().await?;
        let history = head.history(100).await?;
        assert_eq!(history.len(), 2);

        let table = head.view(100).await?;
        assert!(!table.has_checkpoint());
        assert_eq!(table.history.len(), 2);

        Ok(())
    }

    /*
    #[test]
    fn appends_checkpoint_log() -> CalicoResult<()> {
        let temp_logdir = tempdir().unwrap();

        let log_config = LogConfig {
            log_path: temp_logdir.path()
        };

        let log = TransactionLog::init(&log_config)?;
        test_commit!(&log, 1, &vec![F("b", 2)])?;
        test_commit!(&log, 2, &vec![F("b", 2)])?;
        let saved_commit = test_commit!(&log, 3, &vec![F("c", 2)])?;
        log.create_checkpoint(&saved_commit.commit_id, &vec![F("d", 4)])?;
        test_commit!(&log, 4, &vec![F("e", 2)])?;

        let history = log.head_mainline()?.history()?;
        assert_eq!(history.len(), 5);

        let table = log.head_mainline()?.view()?;
        let files = table.files()?;

        assert!(log.has_checkpoint(&saved_commit.commit_id));
        assert!(!log.has_checkpoint(&log.head_mainline()?.commit_id));

        assert_eq!(files.len(), 2);
        assert_eq!(files[0].file_path, "d");
        assert_eq!(files[1].file_path, "e");

        Ok(())
    }

    #[test]
    fn timetravel() -> CalicoResult<()> {
        let temp_logdir = tempdir().unwrap();

        let log_config = LogConfig {
            log_path: temp_logdir.path()
        };

        let log = TransactionLog::init(&log_config)?;
        test_commit!(&log, 1, &vec![F("a", 1)])?;
        test_commit!(&log, 2, &vec![F("b", 2)])?;
        let x = test_commit!(&log, 3, &vec![F("c", 2)])?;
        test_commit!(&log, 4, &vec![F("d", 2)])?;
        test_commit!(&log, 5, &vec![F("e", 2)])?;
        test_commit!(&log, 6, &vec![F("f", 2)])?;
        test_commit!(&log, 7, &vec![F("g", 2)])?;
        
        let history = log.head_mainline()?.history()?;
        assert_eq!(history.len(), 7);

        let history = log.head_mainline()?.at_timestamp(3)?.history()?;
        assert_eq!(history.len(), 3);

        let history = log.get_commit(&x.commit_id)?.history()?;
        assert_eq!(history.len(), 3);


        let table = log.head_mainline()?.at_timestamp(3)?.view()?;
        let files = table.files()?;

        assert_eq!(files.len(), 3);
        assert_eq!(files[0].file_path, "a");
        assert_eq!(files[1].file_path, "b");
        assert_eq!(files[2].file_path, "c");

        Ok(())
    }

    #[test]
    fn timetravel_with_checkpoint() -> CalicoResult<()> {
        let temp_logdir = tempdir().unwrap();

        let log_config = LogConfig {
            log_path: temp_logdir.path()
        };

        let log = TransactionLog::init(&log_config).unwrap();

        test_commit!(&log, 1, &vec![F("a", 1)])?;
        test_commit!(&log, 2, &vec![F("b", 2)])?;
        let saved_commit = test_commit!(&log, 3, &vec![F("c", 2)])?;
        log.create_checkpoint(&saved_commit.commit_id, &vec![F("d", 4)])?;
        test_commit!(&log, 4, &vec![F("d", 2)])?;
        test_commit!(&log, 5, &vec![F("e", 2)])?;
        test_commit!(&log, 6, &vec![F("f", 2)])?;
        test_commit!(&log, 7, &vec![F("g", 2)])?;

        assert_eq!(log.head_mainline()?.at_timestamp(2)?.history()?.len(), 2);
        assert_eq!(log.head_mainline()?.at_timestamp(3)?.history()?.len(), 4);
        assert_eq!(log.head_mainline()?.at_timestamp(4)?.history()?.len(), 5);
        assert_eq!(log.head_mainline()?.at_timestamp(2)?.view()?.files()?.len(), 2);
        assert_eq!(log.head_mainline()?.at_timestamp(3)?.view()?.files()?.len(), 1);
        assert_eq!(log.head_mainline()?.at_timestamp(4)?.view()?.files()?.len(), 2);

        Ok(())
    }
 */
}