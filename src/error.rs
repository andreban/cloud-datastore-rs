use std::error::Error;
use std::fmt::Display;

use crate::TryFromEntityError;
use http::uri::InvalidUri;
use http::Error as HttpError;
use tonic::transport::Error as TransportError;
use tonic::Status;

#[derive(Debug)]
pub enum CloudDatastoreError {
    GrcpError(Status),
    EntityConversionError(TryFromEntityError),
    TransportError(TransportError),
    InvalidUri(InvalidUri),
    HttpError(HttpError),
}

impl Error for CloudDatastoreError {}

impl Display for CloudDatastoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CloudDatastoreError::GrcpError(status) => write!(f, "gRPC error: {}", status),
            CloudDatastoreError::EntityConversionError(error) => {
                write!(f, "Entity conversion error: {}", error)
            }
            CloudDatastoreError::TransportError(error) => {
                write!(f, "Transport error: {}", error)
            }
            CloudDatastoreError::HttpError(error) => write!(f, "HTTP error: {}", error),
            CloudDatastoreError::InvalidUri(error) => write!(f, "Invalid URI: {}", error),
        }
    }
}

impl From<Status> for CloudDatastoreError {
    fn from(status: Status) -> Self {
        CloudDatastoreError::GrcpError(status)
    }
}

impl From<TryFromEntityError> for CloudDatastoreError {
    fn from(error: TryFromEntityError) -> Self {
        CloudDatastoreError::EntityConversionError(error)
    }
}

impl From<TransportError> for CloudDatastoreError {
    fn from(error: TransportError) -> Self {
        CloudDatastoreError::TransportError(error)
    }
}

impl From<HttpError> for CloudDatastoreError {
    fn from(error: HttpError) -> Self {
        CloudDatastoreError::HttpError(error)
    }
}

impl From<InvalidUri> for CloudDatastoreError {
    fn from(error: InvalidUri) -> Self {
        CloudDatastoreError::InvalidUri(error)
    }
}
