pub mod result {
    use std::{error::Error, fmt::Display};

    #[derive(Debug)]
    pub enum IndigoError {
        ReqwestError(reqwest::Error)
    }

    impl Display for IndigoError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self)
        }
    }
    impl Error for IndigoError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            match *self {
                Self::ReqwestError(ref e) => Some(e),
//                _ => None
            }
        }
    }

    impl From<reqwest::Error> for IndigoError {
        fn from(err: reqwest::Error) -> Self {
            IndigoError::ReqwestError(err)
        }
    }    
    
    pub type IndigoResult<T> = Result<T, IndigoError>;
}
