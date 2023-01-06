pub mod result {
    use std::{error::Error, fmt::Display, io};

    #[derive(Debug)]
    pub enum IndigoError {
        RobotForbidden,
        ReqwestError(reqwest::Error),
        IoError(io::Error),
        Utf8Error(std::str::Utf8Error)
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
                Self::IoError(ref e) => Some(e),
                Self::Utf8Error(ref e) => Some(e),
                _ => None
            }
        }
    }

    impl From<io::Error> for IndigoError {
        fn from(err: io::Error) -> Self {
            IndigoError::IoError(err)
        }

    }
    impl From<reqwest::Error> for IndigoError {
        fn from(err: reqwest::Error) -> Self {
            IndigoError::ReqwestError(err)
        }
    }    

    impl From<std::str::Utf8Error> for IndigoError {
        fn from(err: std::str::Utf8Error) -> Self {
            IndigoError::Utf8Error(err)
        }

    }

    pub type IndigoResult<T> = Result<T, IndigoError>;
}
