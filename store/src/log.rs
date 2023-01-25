use async_recursion::async_recursion;
use bytes::Bytes;
use futures::{TryStreamExt, future};
use log::info;
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use prost::Message;
use rand::Rng;
use std::ops::Deref;
use std::sync::Arc;

use crate::protocol;
use calico_shared::result::{CalicoResult, CalicoError};

type BranchRef<'a> = &'a str;
pub const MAIN: &str = "main";


#[derive(Debug)]
pub enum TableAction {
    Checkpoint(protocol::Checkpoint),
    Commit(protocol::Commit)
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
                let cp:Checkpoint = log.get_checkpoint(&commit.commit_id).await?.clone();

                return Ok(TableView {
                    checkpoint: Some(cp),
                    history: since_checkpoint
                });

            } else {
                since_checkpoint.push(commit.clone());
            }
        }

        since_checkpoint.reverse();
        
        // no checkpoint found, return what we have
        // todo: should we warn if we didn't hit end of history?
        return Ok(TableView {
            checkpoint: None,
            history: since_checkpoint
        });
    }

    pub fn actions(&self) -> Vec<TableAction> {
        let mut result:Vec<TableAction> = Vec::with_capacity(self.history.len() + 1);

        if let Some(checkpoint) = &self.checkpoint {
            result.push(TableAction::Checkpoint(checkpoint.checkpoint.clone()));
        }
        for commit in &self.history {
            result.push(TableAction::Commit(commit.commit.clone()))
        }

        result
    }
}



#[derive(Clone, Debug)]
pub struct Commit<'a>{
    pub commit: protocol::Commit,
    log: &'a TransactionLog
}

impl Deref for Commit<'_> {
    type Target = protocol::Commit;
    fn deref(&self) -> &protocol::Commit { &self.commit }
}

impl Commit<'_> {
    pub async fn history(&self, max_history: u32) -> CalicoResult<Vec<Commit>> {
        let mut max_history = max_history;
        let mut history:Vec<Commit> = Vec::new();   
        let mut next_id = self.parent_id.to_vec();

        history.push(self.clone());
        while max_history > 0 {

            match self.log.get_commit(&next_id).await {
                Ok(commit) => {
                    next_id = commit.parent_id.to_vec();
                    history.push(commit);
                }
                Err(_) => {
                    return Ok(history);
                }
            }
            max_history -= 1;
        }

        Ok(history)
    }

    pub async fn view(&self, max_history: u32) -> CalicoResult<TableView> {
        let history = self.history(max_history).await?;
        TableView::from_history(self.log, history).await
    }

    // Determines whether there is an existing checkpoint for this commit
    pub async fn has_checkpoint(&self) -> CalicoResult<bool> {
        self.log.has_checkpoint(&self.commit_id).await
    }

    pub async fn get_checkpoint(&self) -> CalicoResult<Checkpoint> {
        self.log.get_checkpoint(&self.commit_id).await
    }

    pub async fn parent(&self) -> CalicoResult<Commit> {
        self.log.get_commit(&self.parent_id).await
    }

    pub async fn at_checkpoint(&self) -> CalicoResult<Commit> {
        todo!()
    }

    // walks the default parent heirarchy back until it finds the commit that 
    // represents the state of this lineage at the specified timestamp, ie the
    // first commit found at a timestamp equal to or less than <timestamp>
    pub async fn at_timestamp(&self, _timestamp: u64) -> CalicoResult<Commit> {
        let mut _candidate_commit = self;
        let mut _candidate_timestamp = self.timestamp;

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
    checkpoint: protocol::Checkpoint,
    log: &'a TransactionLog
}

impl Checkpoint<'_> {
    // Returns the commit object this checkpoint corresponds to.
    pub async fn commit(&self) -> CalicoResult<Commit> {
        self.log.get_commit(&self.checkpoint.commit_id).await
    }
}

impl Deref for Checkpoint<'_> {
    type Target = protocol::Checkpoint;
    fn deref(&self) -> &protocol::Checkpoint { &self.checkpoint }
}

#[derive(Clone, Debug)]
pub enum ReferencePoint {
    // nth Ancestor of a reference point
    Ancestor(Box<ReferencePoint>, u64),
    // Specific commit
    Commit(Vec<u8>),
    // Head of main
    Main,
    // Immediate parent of a reference point
    Parent(Box<ReferencePoint>),
    // Rewinds from a reference point to find the last checkpoint
    PriorCheckpoint(Box<ReferencePoint>),
    // Named reference point
    Ref(String),
    // Rewinds from a reference point to find the state at timestamp
    TimestampFrom(Box<ReferencePoint>, u64),
}

impl From<protocol::Commit> for ReferencePoint {
    fn from(commit: protocol::Commit) -> Self {
        ReferencePoint::Commit(commit.commit_id.to_vec())
    }
}

impl ReferencePoint {
    /*
    // TODO: implement support for reference point parsing

    // main@^                     One commit back HEAD on main
    // main@{10}                  Ten commits back HEAD on main
    // main@[20221026T220447Z]    Rewind master to a specific timestamp
    
    fn parse(reference_str: &str) -> Self {
        todo!()
    }
     */
}

#[derive(Clone, Debug)]
pub struct TransactionLog {
    object_store: Arc<dyn ObjectStore>
}

impl TransactionLog {
    pub async fn init(object_store: Arc<dyn ObjectStore>) -> CalicoResult<TransactionLog> {
        let log = TransactionLog{ 
            object_store,
        };

        log.init_dir(Self::REF_DIR).await?;
        log.init_dir(Self::TMP_DIR).await?;
        log.init_dir(Self::COMMIT_DIR).await?;
        log.init_dir(Self::CHECKPOINT_DIR).await?;

        log.init_log().await?;

        log.init_branch(MAIN).await?;

        Ok(log)
    }

    pub async fn open(object_store: Arc<dyn ObjectStore>) -> CalicoResult<TransactionLog> {
        // check to see if the path exists and is initialized, panic if not
        let path: ObjectStorePath = Self::META_FILE.try_into().unwrap();
        let _meta = object_store.head(&path).await?;

        Ok(TransactionLog { object_store })
    }

    pub async fn head<'a>(&self, branch: BranchRef<'a>) -> CalicoResult<Commit> {
        let commit_id = self.head_id(branch).await?;
        let commit = self.get_commit(&commit_id).await?;

        Ok(commit)
    }

    pub async fn head_main(&self) -> CalicoResult<Commit> {
        self.head(MAIN).await
    }

    pub async fn head_id<'a>(&self, branch: BranchRef<'a>) -> CalicoResult<Vec<u8>> 
    {
        let prot_ref = self.get_last_ref(branch).await?;

        Ok(prot_ref.commit_id)
    }

    pub async fn head_id_main(&self) -> CalicoResult<Vec<u8>> {
        self.head_id(MAIN).await
    }

    pub async fn init_branch<'a>(&self, branch: BranchRef<'a>) -> CalicoResult<protocol::Ref> {
        let path = Self::branch_path(branch)?;
        self.init_dir(&path.to_string().as_str()).await?;

        let mut prot_ref = protocol::Ref::default();

        prot_ref.label = branch.to_string();
        prot_ref.ref_seq = 0;
        prot_ref.commit_id = [0;20].to_vec();

        self.put_ref(branch, prot_ref).await
    }

    #[async_recursion]
    pub async fn find_commit(&self, reference: &ReferencePoint) -> CalicoResult<Commit> {
        match reference {
            ReferencePoint::Ref(tag) => {
                let reference = self.get_last_ref(&tag).await?;
                self.get_commit(&reference.commit_id).await
            },
            ReferencePoint::Commit(commit_id) => {
                self.get_commit(&commit_id).await
            },            
            ReferencePoint::TimestampFrom(reference_point, timestamp) => {
                let commit = self.find_commit(reference_point).await?;
                let timestamp_commit = commit.at_timestamp(*timestamp).await?.clone();
                Ok(Commit {
                    commit: timestamp_commit.commit,
                    log: self
                })
            },
            ReferencePoint::Ancestor(_, _) => {
                todo!()
            },
            ReferencePoint::Main => {
                self.head_main().await
            }
            ReferencePoint::Parent(reference_point) => {
                let commit = self.find_commit(reference_point).await?;
                let parent = commit.parent().await?.to_owned();
                Ok(Commit {
                    commit: parent.commit,
                    log: self
                })
            },
            ReferencePoint::PriorCheckpoint(reference_point) => {
                let commit = self.find_commit(reference_point).await?;
                let parent = commit.at_checkpoint().await?.to_owned();
                Ok(Commit {
                    commit: parent.commit,
                    log: self
                })
            }
        }
    }

    pub async fn get_commit(&self, commit_id: &Vec<u8>) -> CalicoResult<Commit> {
        let path = Self::commit_path(&commit_id)?;

        // todo: support a local in-memory object store cache

        info!("{}: reading commit record", hex::encode(&commit_id));
        let result = self.object_store.get(&path).await?;
        let bytes = result.bytes().await?;
        let commit:protocol::Commit = protocol::Commit::decode(bytes)?;

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
                         columns: Vec<String>,
                         _column_expressions: Vec<(String, String)>,
                         tile_files: Vec<protocol::TileFiles>) -> CalicoResult<Commit> {
        
        let mut commit = protocol::Commit::default();

        // todo: any way to avoid vec<u8> for this since we have fixed size?
        commit.commit_id = rand::thread_rng().gen::<[u8; 20]>().to_vec();
    
        commit.parent_id = parent_id.to_vec();
        commit.timestamp = commit_timestamp;
        commit.columns = columns;
        commit.tile_files = tile_files;

        let expected_len = commit.encoded_len();

        let mut buf = Vec::with_capacity(18);
        commit.encode(&mut buf)?;
        assert_eq!(expected_len, buf.len());

        let path = Self::commit_path(&commit.commit_id)?;
        let data = Bytes::from(buf);
        
        info!("{}: storing commit record", hex::encode(&commit.commit_id));

        // Note that this does not use the 'put_if_not_exist' because it is 
        // expected that a [u8; 20] rand is always unique.
        self.object_store.put(&path, data).await?;

        Ok(Commit {
            commit,
            log: &self
        })
    }

    // finds the last instance of a branch ref
    async fn find_last_ref<'a>(&self, branch: BranchRef<'a>) -> CalicoResult<ObjectStorePath> {
        let prefix: ObjectStorePath = format!("{}/{}/", Self::REF_DIR, branch).try_into().unwrap();

        let mut paths = self.object_store.list(Some(&prefix))
            .await?
            .map_ok(|meta| meta.location)
            .try_filter(|path| future::ready(!path.to_string().ends_with(Self::META_FILE)))
            .try_collect::<Vec<ObjectStorePath>>().await?;
        
        paths.sort();

        paths.last()
            .map(|path| path.clone())
            .ok_or(CalicoError::BranchNotFound(branch.to_string()))
    }

    async fn get_last_ref<'a>(&self, branch: BranchRef<'a>) -> CalicoResult<protocol::Ref> {
        let ref_path = self.find_last_ref(branch).await?;

        // todo: support a local in-memory object store cache

        info!("{}: reading branch ref {}", branch, ref_path);

        let result = self.object_store.get(&ref_path).await?;
        let bytes = result.bytes().await?;
        let prot_ref:protocol::Ref = protocol::Ref::decode(bytes)?;

        Ok(prot_ref)
    }

    async fn put_ref<'a>(&self, branch: BranchRef<'a>, prot_ref: protocol::Ref) -> CalicoResult<protocol::Ref> {
        let expected_len = prot_ref.encoded_len();

        let mut buf = Vec::with_capacity(18);
        prot_ref.encode(&mut buf)?;
        assert_eq!(expected_len, buf.len());

        let path = Self::ref_path(branch, prot_ref.ref_seq)?;

        let data = Bytes::from(buf);

        info!("{}: writing branch ref {}", branch, path);
        self.put_if_not_exists(&path, data).await?;
        
        Ok(prot_ref)
    }

    // Perform a fast-forward only merge on a branch to a commit.
    pub async fn fast_forward<'a>(&self,
                                  branch: BranchRef<'a>,
                                  commit_id: &Vec<u8>) -> CalicoResult<protocol::Ref> {
        let mut prot_ref = self.get_last_ref(branch).await?;

        // TODO: make sure we're actually fast forwarding!

        prot_ref.ref_seq += 1;
        prot_ref.commit_id = commit_id.to_vec();

        self.put_ref(branch, prot_ref).await
    }

    // Creates a new checkpoint based on the specified commit using the set of objects referenced for the snapshot
    pub async fn create_checkpoint(&self, 
                                   commit_id: &Vec<u8>,
                                   timestamp: u64,
                                   tile_files: &Vec<protocol::TileFiles>) -> CalicoResult<Checkpoint> {

        let mut checkpoint = protocol::Checkpoint::default();

        checkpoint.commit_id = commit_id.to_vec();
        checkpoint.timestamp = timestamp;
        checkpoint.tile_files = tile_files.to_vec();

        let path = Self::checkpoint_path(commit_id)?;

        // todo: support checkpointing metadata
        let expected_len = checkpoint.encoded_len();

        let mut buf = Vec::with_capacity(18);
        checkpoint.encode(&mut buf)?;
        assert_eq!(expected_len, buf.len());
        let data = Bytes::from(buf);

        info!("{}: writing checkpoint to {}",  hex::encode(&commit_id), path);
        self.put_if_not_exists(&path, data).await?;
            
        Ok(Checkpoint {
            log: self,
            checkpoint
        })
    }

    // Determines whether there is an existing checkpoint for the passed in commit
    pub async fn has_checkpoint(&self, commit_id: &Vec<u8>) -> CalicoResult<bool> {
        let path = Self::checkpoint_path(commit_id)?;

        Ok(self.object_store.head(&path).await.is_ok())
    }

    pub async fn get_checkpoint(&self, commit_id: &Vec<u8>) -> CalicoResult<Checkpoint> {
        let path = Self::checkpoint_path(commit_id)?;
        let result = self.object_store.get(&path).await?;
        let bytes = result.bytes().await?;
        let checkpoint:protocol::Checkpoint = protocol::Checkpoint::decode(bytes)?;

        Ok(Checkpoint {
            log: self,
            checkpoint
        })
    }

    //--------------------------------------------------------------------------
    // Object Store Helpers
    // todo: move to object.rs?
    //--------------------------------------------------------------------------
    
    const REF_DIR: &'static str = "refs";
    const TMP_DIR: &'static str = "tmp";
    const COMMIT_DIR: &'static str = "commit";
    const CHECKPOINT_DIR: &'static str = "checkpoints";

    const META_FILE: &'static str = ".meta";

    fn ref_path<'a>(branch: BranchRef<'a>, ref_seq: u64) -> CalicoResult<ObjectStorePath> {
        let path: ObjectStorePath = format!("{}/{}/{:08x}", Self::REF_DIR, branch, ref_seq).try_into().unwrap();
        Ok(path)
    }

    fn branch_path<'a>(branch: BranchRef<'a>) -> CalicoResult<ObjectStorePath> {
        let path: ObjectStorePath = format!("{}/{}", Self::REF_DIR, branch).try_into().unwrap();
        Ok(path)
    }

    fn tmp_path() -> CalicoResult<ObjectStorePath> {
        let tmp_id = rand::thread_rng().gen::<[u8; 20]>().to_vec();
        let path: ObjectStorePath = format!("{}/{}", Self::TMP_DIR, hex::encode(&tmp_id)).try_into().unwrap();
        Ok(path)
    }

    fn commit_path(commit_id: &Vec<u8>) -> CalicoResult<ObjectStorePath> {
        let path: ObjectStorePath = format!("{}/{}", Self::COMMIT_DIR, hex::encode(&commit_id)).try_into().unwrap();
        Ok(path)
    }

    fn checkpoint_path(commit_id: &Vec<u8>) -> CalicoResult<ObjectStorePath> {
        let path: ObjectStorePath = format!("{}/{}", Self::CHECKPOINT_DIR, hex::encode(&commit_id)).try_into().unwrap();
        Ok(path)
    }

    // helper to perform a safe put to the object store. write to tmp, rename 
    // to destination. If it fails try to cleanup the temp, but don't care if 
    // you can't

    async fn put_if_not_exists(&self, path: &ObjectStorePath, data: Bytes) -> CalicoResult<()> {
        let tmp_path = Self::tmp_path()?;

        self.object_store.put(&tmp_path, data).await?;

        // todo: support tmp cleanup in GC
        
        match self.object_store.copy_if_not_exists(&tmp_path, &path).await {
            Ok(_) => Ok(()),
            Err(e) => {
                // clean up 
                self.object_store.delete(&tmp_path).await?;
                Err(CalicoError::ObjectStoreError(e))
            },
        }

    }

    async fn init_dir<'a>(&self, path:&'a str) -> CalicoResult<()> {
        let path: ObjectStorePath = format!("{}/{}", path, Self::META_FILE).try_into().unwrap();
        let data = Bytes::from("{}");

        self.object_store.put(&path, data).await?;

        Ok(())
    }

    async fn init_log(&self) -> CalicoResult<()> {
        let path: ObjectStorePath = Self::META_FILE.try_into().unwrap();
        let data = Bytes::from("{}");

        self.object_store.put(&path, data).await?;

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use object_store::ObjectStore;
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;
    use std::fs;
    use std::sync::Arc;

    use crate::log::{TransactionLog, Commit };
    use crate::protocol;
    use calico_shared::result::CalicoResult;

    use super::MAIN;

    #[tokio::test]
    async fn create_new_transaction_log() -> CalicoResult<()> {
        let temp_logdir = tempdir().unwrap();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(temp_logdir.path())?);
        let log = TransactionLog::init(object_store).await?;

        let head_result = log.head_main().await;
        assert!(head_result.is_err());

        Ok(())
    }

    fn test_file(timestamp: u64, partition_num: u64, column_group: &str, filename: &str) -> protocol::TileFiles {
        protocol::TileFiles {
            tile: Some(protocol::Tile {
                partition_num,
                column_group: column_group.to_string(),
            }),
            file: vec![protocol::File {
                file_path: filename.to_string(),
                file_type: protocol::FileType::Data as i32,
                file_size: 1,
                update_time: timestamp
            }]
        }
    }
    async fn test_commit_push<'a>(log:&'a TransactionLog, timestamp: u64, partition_num: u64, column_group: &str, filename: &str) -> Commit<'a> {
        let cols = vec!["a".to_string()];
        let col_expr = vec![("a".to_string(), "$new".to_string())];
        let head_id = log.head_id_main().await.unwrap();
        let file = test_file(timestamp, partition_num, column_group, filename);

        let commit = log.create_commit(
            &head_id.to_vec(), 
            None, 
            None, 
            None, 
            timestamp, 
            cols, 
            col_expr, 
            vec![file]).await.unwrap();

        let _new_head = log.fast_forward(MAIN, &commit.commit_id).await.unwrap();

        commit
    }

    #[tokio::test]
    async fn round_trip() -> CalicoResult<()> {
        let temp_logdir = tempdir().unwrap();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(temp_logdir.path())?);
        let log = TransactionLog::init(object_store).await?;

        let commit = test_commit_push(&log, 1, 0, "x", "a").await;
        let commit_dir = temp_logdir.path().join("commit");
        let commit_file = fs::read_dir(commit_dir).unwrap().nth(0).unwrap().unwrap();
        let commit_size = fs::metadata(commit_file.path()).unwrap().len();

        assert!(commit_size > 0);

        let read_commit = log.get_commit(&commit.commit_id).await?;

        assert_eq!(read_commit.timestamp, 1);
        assert_eq!(read_commit.tile_files[0].file[0].file_path, "a");

        Ok(())
    }



    #[tokio::test]
    async fn appends_in_log() -> CalicoResult<()> {

        let temp_logdir = tempdir().unwrap();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(temp_logdir.path())?);
        let log = TransactionLog::init(object_store).await?;

        test_commit_push(&log, 1, 0, "x", "a").await;

        let head = log.head_main().await?;
        let history = head.history(100).await.unwrap();
        assert_eq!(history.len(), 1);

        let table = head.view(100).await.unwrap();
        assert!(!table.has_checkpoint());
        assert_eq!(table.history.len(), 1);

        test_commit_push(&log, 1, 0, "x", "b").await;

        let head = log.head_main().await?;
        let history = head.history(100).await.unwrap();
        assert_eq!(history.len(), 2);

        let table = head.view(100).await.unwrap();
        assert!(!table.has_checkpoint());
        assert_eq!(table.history.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn appends_checkpoint_log() -> CalicoResult<()> {
        let temp_logdir = tempdir().unwrap();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(temp_logdir.path())?);
        let log = TransactionLog::init(object_store).await?;

        test_commit_push(&log, 1, 0, "x", "a").await;
        test_commit_push(&log, 2, 0, "x", "b").await;
        let saved_commit = test_commit_push(&log, 3, 0, "x", "c").await;
        test_commit_push(&log, 4, 0, "x", "e").await;
        log.create_checkpoint(&saved_commit.commit_id, 5, &vec![test_file(5, 0, "x", "d")]).await?;

        let head = log.head_main().await?;
        let history = head.history(100).await?;
        assert_eq!(history.len(), 4);

        let table = head.view(100).await?;
        assert!(table.has_checkpoint());
        assert_eq!(table.history.len(), 1);

        assert!(log.has_checkpoint(&saved_commit.commit_id).await?);
        assert!(saved_commit.has_checkpoint().await?);

        assert!(!head.has_checkpoint().await?);

 //       assert_eq!(head.parent().await?, saved_commit);
        assert!(head.parent().await?.has_checkpoint().await?);

        Ok(())
    }

    #[tokio::test]
    async fn fails_duplicate_checkpoint() -> CalicoResult<()> {
        let temp_logdir = tempdir().unwrap();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(temp_logdir.path())?);
        let log = TransactionLog::init(object_store).await?;

        let saved_commit = test_commit_push(&log, 1, 0, "x", "a").await;
        log.create_checkpoint(&saved_commit.commit_id, 2, &vec![test_file(5, 0, "x", "d")]).await?;
        let checkpoint_result = log.create_checkpoint(&saved_commit.commit_id, 3, &vec![test_file(5, 0, "x", "d")]).await;

        assert!(checkpoint_result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn open_uninitialized_log_fails() -> CalicoResult<()> {
        let temp_logdir = tempdir().unwrap();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(temp_logdir.path())?);
        let res = TransactionLog::open(object_store.clone()).await;
        assert!(res.is_err());

        let log = TransactionLog::init(object_store.clone()).await?;
        let commit = test_commit_push(&log, 1, 0, "x", "a").await;

        let log = TransactionLog::open(object_store.clone()).await?;
        let head = log.head_main().await?;

        assert_eq!(head.commit_id, commit.commit_id);

        Ok(())
    }

    /*

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