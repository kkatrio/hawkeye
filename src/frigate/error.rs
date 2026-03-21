use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum ClientError {
    Frigate(FrigateError),
    Io(std::io::Error),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Frigate(e) => write!(f, "Frigate error: {e}"),
            ClientError::Io(e) => write!(f, "Io error: {e}"),
        }
    }
}

impl From<std::io::Error> for ClientError {
    fn from(error: std::io::Error) -> Self {
        ClientError::Io(error)
    }
}

impl From<FrigateError> for ClientError {
    fn from(error: FrigateError) -> Self {
        ClientError::Frigate(error)
    }
}

impl From<reqwest::Error> for ClientError {
    fn from(error: reqwest::Error) -> Self {
        ClientError::Frigate(FrigateError::RequestFailed(error))
    }
}

#[derive(Debug)]
pub enum FrigateError {
    LoginFailed(reqwest::Error),
    TokenMissing,
    RequestFailed(reqwest::Error),
    SerializationFailed(serde_json::Error),
    BadResponse {
        status: reqwest::StatusCode,
        body: String,
    },
}

impl std::error::Error for FrigateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            FrigateError::LoginFailed(e) => Some(e),
            FrigateError::TokenMissing => None,
            FrigateError::RequestFailed(e) => Some(e),
            FrigateError::SerializationFailed(e) => Some(e),
            FrigateError::BadResponse { .. } => None,
        }
    }
}

impl fmt::Display for FrigateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrigateError::LoginFailed(e) => write!(f, "Login failed: {e}"),
            FrigateError::TokenMissing => write!(f, "Token missing"),
            FrigateError::RequestFailed(e) => write!(f, "Request failed: {e}"),
            FrigateError::SerializationFailed(e) => write!(f, "Serialization failed: {e}"),
            FrigateError::BadResponse { status, body } => {
                write!(f, "Received bad response, status: {status}, body: {body}")
            }
        }
    }
}

impl From<serde_json::Error> for FrigateError {
    fn from(error: serde_json::Error) -> Self {
        FrigateError::SerializationFailed(error)
    }
}

impl From<reqwest::Error> for FrigateError {
    fn from(error: reqwest::Error) -> Self {
        FrigateError::RequestFailed(error)
    }
}
