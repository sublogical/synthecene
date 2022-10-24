pub mod protocol {
    // todo: not windows specific (gah)
    include!(concat!(env!("OUT_DIR"), "\\calico.protocol.rs"));
}

mod writer;

pub mod log;
pub mod object;
pub mod operations;
pub mod partition;
pub mod table;
// pub mod reader;

pub struct CalicoSchema {
}

pub mod result {
    use std::{error::Error, fmt::Display};

    use arrow::error::ArrowError;
    use parquet::errors::ParquetError;
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
        ParquetError(ParquetError),

        PartitionError(&'static str),
        TransactionLogAlreadyIntialized(String),
        TransactionLogNotPresent(String),
        UnknownColumn(String),
        UnknownColumnGroup(String),
    }

    impl Display for CalicoError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self)
        }
    }
    impl Error for CalicoError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            match *self {
                Self::ArrowMessedUp(ref e) => Some(e),
                Self::JoinFailed(ref e) => Some(e),
                Self::EncodeError(ref e) => Some(e),
                Self::DecodeError(ref e) => Some(e),
                Self::ObjectStoreError(ref e) => Some(e),
                Self::ParquetError(ref e) => Some(e),
                _ => None
            }
        }
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
    impl From<ParquetError> for CalicoError {
        fn from(err: ParquetError) -> Self {
            CalicoError::ParquetError(err)
        }
    }    
    impl From<ObjectStoreError> for CalicoError {
        fn from(err: ObjectStoreError) -> Self {
            CalicoError::ObjectStoreError(err)
        }
    }    

    pub type CalicoResult<T> = Result<T, CalicoError>;
}
