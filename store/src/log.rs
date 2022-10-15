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

use crate::log_protocol::*;
use crate::log_protocol::Commit as ProtocolCommit;
use crate::log_protocol::Ref as ProtocolRef;
use crate::partition::ColumnRef;
use crate::result::{CalicoResult, CalicoError};

type BranchRef<'a> = &'a str;
pub const MAINLINE: &str = "mainline";

pub struct LogConfig<'a> {
    log_path: &'a Path
}

pub struct TableView {

}

impl TableView {
    pub fn files(&self) -> CalicoResult<Vec<File>> {
        Ok(vec![])
    }

    fn from_history(history: Vec<Commit>) -> CalicoResult<&TableView> {
        todo!()
    }
}



pub struct Commit<'a>{
    commit: ProtocolCommit,
    log: &'a TransactionLog
}

impl Deref for Commit<'_> {
    type Target = ProtocolCommit;
    fn deref(&self) -> &ProtocolCommit { &self.commit }
}

impl Commit<'_> {
    fn history(&self) -> CalicoResult<Vec<Commit>> {
        todo!("Walk backwards in history up the series of commits");
        Ok(vec![])
    }

    fn view(&self) -> CalicoResult<&TableView> {
        let history = self.history()?;
        TableView::from_history(history)
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

    pub fn head(&self,
                branch: BranchRef) -> CalicoResult<&Commit> {
        todo!("find the head tag and return the commit it references");
    }

    pub fn head_mainline(&self) -> CalicoResult<&Commit> {
        self.head(MAINLINE)
    }

    pub async fn head_id<'a>(&self, branch: BranchRef<'a>) -> CalicoResult<Vec<u8>> 
    {
        let mut prot_ref = self.get_last_ref(branch).await?;

        Ok(prot_ref.commit_id)
    }

    pub async fn head_id_mainline(&self) -> CalicoResult<Vec<u8>> {
        self.head_id(MAINLINE).await
    }

    pub async fn init_branch<'a>(&self, branch: BranchRef<'a>) -> CalicoResult<ProtocolRef> {
        let mut prot_ref = ProtocolRef::default();

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
        let commit:ProtocolCommit = ProtocolCommit::decode(bytes)?;

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
                         files: Vec<File>) -> CalicoResult<Commit> {
        
        let mut commit = ProtocolCommit::default();

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
        
        info!("{}: storing commit record", hex::encode(&commit.commit_id));
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

    async fn get_last_ref<'a>(&self, branch: BranchRef<'a>) -> CalicoResult<ProtocolRef> {
        let ref_path = self.find_last_ref(branch).await?;

        // todo: support a local in-memory object store cache

        info!("{}: reading branch ref {}", branch, ref_path);

        let result = self.object_store.get(&ref_path).await?;
        let bytes = result.bytes().await?;
        let prot_ref:ProtocolRef = ProtocolRef::decode(bytes)?;

        Ok(prot_ref)
    }

    async fn put_ref<'a>(&self, branch: BranchRef<'a>, prot_ref: ProtocolRef) -> CalicoResult<ProtocolRef> {
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
                                  commit_id: &Vec<u8>) -> CalicoResult<ProtocolRef> {
        let mut prot_ref = self.get_last_ref(branch).await?;

        // TODO: make sure we're actually fast forwarding!

        prot_ref.ref_seq += 1;
        prot_ref.commit_id = commit_id.to_vec();

        self.put_ref(branch, prot_ref).await
    }

    // Creates a new checkpoint based on the specified commit using the set of objects referenced for the snapshot
    pub fn create_checkpoint(&self, 
                             commit_id: &Vec<u8>,
                             files: &Vec<File>) -> CalicoResult<Checkpoint> {
        Ok(Checkpoint::default())
    }

    // Determines whether there is an existing checkpoint for the passed in commit
    fn has_checkpoint(&self, 
                      commit_id: &[u8]) -> bool {
        todo!("check to see if there is a checkpoint associated with this commit");
    }

    fn checkpoint(&self, ) -> CalicoResult<Checkpoint> {
        todo!("load the actual checkpoint");
    }

}


#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use std::{fs, time};

    use crate::log::{TransactionLog, LogConfig, Commit };
    use crate::log_protocol::{File, FileType};
    use crate::result::CalicoResult;

    use super::MAINLINE;

    #[tokio::test]
    async fn create_new_transaction_log() -> CalicoResult<()> {
        let temp_logdir = tempdir().unwrap();

        let log_config = LogConfig {
            log_path: temp_logdir.path()
        };

        let log = TransactionLog::init(&log_config).await?;
        let history = log.head_mainline()?.history()?;
        assert_eq!(history.len(), 0);

        Ok(())
    }

    async fn test_commit_push<'a>(log:&'a TransactionLog, timestamp: u64, filename: &str) -> Commit<'a> {
        let cols = vec!["a".to_string()];
        let col_expr = vec![("a".to_string(), "$new".to_string())];
        let head_id = log.head_id_mainline().await.unwrap();
        let file = File {
            file_path: filename.to_string(),
            file_type: FileType::Data as i32,
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

/*

    #[tokio::test]
    async fn appends_in_log() -> CalicoResult<()> {

        let temp_logdir = tempdir().unwrap();

        let log_config = LogConfig {
            log_path: temp_logdir.path()
        };

        let log = TransactionLog::init(&log_config)?;
        test_commit!(&log, 1, &vec![F("a", 1)])?;

        let history = log.head_mainline()?.history()?;
        assert_eq!(history.len(), 1);

        let table = log.head_mainline()?.view()?;
        assert_eq!(table.files()?.len(), 1);
        assert_eq!(table.files()?[0].file_path, "a");

        test_commit!(&log, 2, &vec![F("b", 2)])?;

        let history = log.head_mainline()?.history()?;
        assert_eq!(history.len(), 2);

        let table = log.head_mainline()?.view()?;
        assert_eq!(table.files()?.len(), 2);
        assert_eq!(table.files()?[1].file_path, "b");

        Ok(())
    }

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