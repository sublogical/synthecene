pub mod result {
    use std::{error::Error, fmt::Display, io};

    use arrow::error::ArrowError;
    use datafusion::error::DataFusionError;
    use parquet::errors::ParquetError;
    use prost::{DecodeError, EncodeError};
    use tokio::task::JoinError;
    use object_store::Error as ObjectStoreError;

    #[derive(Debug)]
    pub enum SyntheceneError {
        RobotForbidden,

        ArrowMessedUp(ArrowError),
        BranchNotFound(String),
        DecodeError(DecodeError),
        EncodeError(EncodeError),
        IoError(io::Error),
        JoinFailed(JoinError),
        KvError(kv::Error),
        ObjectStoreError(ObjectStoreError),

        ParquetError(ParquetError),
        PartitionError(&'static str),
        ReqwestError(reqwest::Error),
        TransactionLogAlreadyIntialized(String),
        TransactionLogNotPresent(String),
        UnknownColumn(String),
        UnknownColumnGroup(String),
        Utf8Error(std::str::Utf8Error),
    }

    impl Display for SyntheceneError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self)
        }
    }
    impl Error for SyntheceneError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            match *self {
                Self::ArrowMessedUp(ref e) => Some(e),
                Self::JoinFailed(ref e) => Some(e),
                Self::EncodeError(ref e) => Some(e),
                Self::DecodeError(ref e) => Some(e),
                Self::ObjectStoreError(ref e) => Some(e),
                Self::ParquetError(ref e) => Some(e),
                Self::ReqwestError(ref e) => Some(e),                
                Self::IoError(ref e) => Some(e),
                Self::Utf8Error(ref e) => Some(e),
                Self::KvError(ref e) => Some(e),
                _ => None
            }
        }
    }

    impl From<SyntheceneError> for DataFusionError {
        fn from(err: SyntheceneError) -> Self {
            DataFusionError::External(Box::new(err))
        }
    }    


    impl From<ArrowError> for SyntheceneError {
        fn from(err: ArrowError) -> Self {
            SyntheceneError::ArrowMessedUp(err)
        }
    }    

    impl From<io::Error> for SyntheceneError {
        fn from(err: io::Error) -> Self {
            SyntheceneError::IoError(err)
        }

    }
    impl From<JoinError> for SyntheceneError {
        fn from(err: JoinError) -> Self {
            SyntheceneError::JoinFailed(err)
        }
    }    
    impl From<EncodeError> for SyntheceneError {
        fn from(err: EncodeError) -> Self {
            SyntheceneError::EncodeError(err)
        }
    }    
    impl From<DecodeError> for SyntheceneError {
        fn from(err: DecodeError) -> Self {
            SyntheceneError::DecodeError(err)
        }
    }    
    impl From<ParquetError> for SyntheceneError {
        fn from(err: ParquetError) -> Self {
            SyntheceneError::ParquetError(err)
        }
    }    
    impl From<reqwest::Error> for SyntheceneError {
        fn from(err: reqwest::Error) -> Self {
            SyntheceneError::ReqwestError(err)
        }
    }    
    impl From<ObjectStoreError> for SyntheceneError {
        fn from(err: ObjectStoreError) -> Self {
            SyntheceneError::ObjectStoreError(err)
        }
    }    

    impl From<std::str::Utf8Error> for SyntheceneError {
        fn from(err: std::str::Utf8Error) -> Self {
            SyntheceneError::Utf8Error(err)
        }

    }

    impl From<kv::Error> for SyntheceneError {
        fn from(err: kv::Error) -> Self  {
            SyntheceneError::KvError(err)
        }

    }
    pub type SyntheceneResult<T> = Result<T, SyntheceneError>;

}

pub mod applications {
    pub const CRAWLER:&str = "synthecene:acquisition:crawler:0";    // crawler
    pub const GLIMMER:&str = "synthecene:serving:glimmer:0";        // long-running agent server
    pub const MEPHESTO:&str = "synthecene:processing:mephesto:0";   // workflow engine
    pub const SIGNAL:&str = "synthecene:processing:signal:0";       // signal processing worker
    pub const VERA:&str = "synthecene:store:vera:0";                // feature store
}

pub mod types;
pub mod partition_stream;

#[cfg(test)]
mod tests {
}
