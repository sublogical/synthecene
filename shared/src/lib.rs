pub mod result {
    use std::{error::Error, fmt::Display, io};

    use arrow::error::ArrowError;
    use datafusion::error::DataFusionError;
    use parquet::errors::ParquetError;
    use prost::{DecodeError, EncodeError};
    use tokio::task::JoinError;
    use object_store::Error as ObjectStoreError;

    #[derive(Debug)]
    pub enum CalicoError {
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
                Self::ReqwestError(ref e) => Some(e),                
                Self::IoError(ref e) => Some(e),
                Self::Utf8Error(ref e) => Some(e),
                Self::KvError(ref e) => Some(e),
                _ => None
            }
        }
    }

    impl From<CalicoError> for DataFusionError {
        fn from(err: CalicoError) -> Self {
            DataFusionError::External(Box::new(err))
        }
    }    


    impl From<ArrowError> for CalicoError {
        fn from(err: ArrowError) -> Self {
            CalicoError::ArrowMessedUp(err)
        }
    }    

    impl From<io::Error> for CalicoError {
        fn from(err: io::Error) -> Self {
            CalicoError::IoError(err)
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
    impl From<reqwest::Error> for CalicoError {
        fn from(err: reqwest::Error) -> Self {
            CalicoError::ReqwestError(err)
        }
    }    
    impl From<ObjectStoreError> for CalicoError {
        fn from(err: ObjectStoreError) -> Self {
            CalicoError::ObjectStoreError(err)
        }
    }    

    impl From<std::str::Utf8Error> for CalicoError {
        fn from(err: std::str::Utf8Error) -> Self {
            CalicoError::Utf8Error(err)
        }

    }

    impl From<kv::Error> for CalicoError {
        fn from(err: kv::Error) -> Self {
            CalicoError::KvError(err)
        }

    }
    pub type CalicoResult<T> = Result<T, CalicoError>;

}


#[cfg(test)]
mod tests {
}
