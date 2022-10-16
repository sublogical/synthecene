pub mod protocol {
    // todo: not windows specific (gah)
    include!(concat!(env!("OUT_DIR"), "\\calico.protocol.rs"));
}

mod writer;

pub mod log;
pub mod object;
//pub mod operations;
pub mod partition;
// pub mod reader;

pub mod result {
    use arrow::error::ArrowError;
    use prost::{DecodeError, EncodeError};
    use tokio::task::JoinError;
    use object_store::Error as ObjectStoreError;

    #[derive(Debug)]
    pub enum CalicoError {
        ArrowMessedUp(ArrowError),
        JoinFailed(JoinError),
        EncodeError(EncodeError),
        DecodeError(DecodeError),
        ObjectStoreError(ObjectStoreError),
        BranchNotFound(String),

        TransactionLogAlreadyIntialized(String),
        TransactionLogNotPresent(String)
    }
    impl From<ArrowError> for CalicoError {
        fn from(err: ArrowError) -> Self {
            CalicoError::ArrowMessedUp(err)
        }
    }    
    impl From<JoinError> for CalicoError {
        fn from(err: JoinError) -> Self {
            CalicoError::JoinFailed(err)
        }
    }    
    impl From<EncodeError> for CalicoError {
        fn from(err: EncodeError) -> Self {
            CalicoError::EncodeError(err)
        }
    }    
    impl From<DecodeError> for CalicoError {
        fn from(err: DecodeError) -> Self {
            CalicoError::DecodeError(err)
        }
    }    
    impl From<ObjectStoreError> for CalicoError {
        fn from(err: ObjectStoreError) -> Self {
            CalicoError::ObjectStoreError(err)
        }
    }    

    pub type CalicoResult<T> = Result<T, CalicoError>;
}
