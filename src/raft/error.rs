use thiserror::Error;
use tonic::Status;

/// A specialized `Result` type for Raft operations.
pub type RaftResult<T> = Result<T, RaftError>;

/// Structured error type used throughout the raft package.
#[derive(Error, Debug)]
pub enum RaftError {
    #[error("Initialization error: {0}")]
    Initialization(String),

    #[error("RPC error from peer {peer}: {status}")]
    Rpc {
        peer: String,
        #[source]
        status: Status,
    },

    #[error("Failed to connect to peer {peer}: {source}")]
    ConnectionFailed {
        peer: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Invalid argument in request: {0}")]
    InvalidArgument(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Non-contiguous log entries supplied. Expected index {expected}, got {actual}")]
    NonContiguousLog { expected: i64, actual: i64 },

    #[error("Stale term state")]
    StaleState,
}

impl RaftError {
    pub(crate) fn missing(field: &str) -> Self {
        Self::InvalidArgument(format!("Missing field {}", field))
    }
}

/// Conversion from our internal `RaftError` to a gRPC `Status`.
/// This is crucial for translating errors into meaningful responses for clients.
impl From<RaftError> for Status {
    fn from(err: RaftError) -> Self {
        match err {
            RaftError::InvalidArgument(msg) => Status::invalid_argument(msg),
            _ => Status::internal(err.to_string()),
        }
    }
}
