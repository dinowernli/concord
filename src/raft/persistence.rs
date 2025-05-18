use crate::raft::raft_common_proto::{Entry, Server};
use crate::raft::raft_persistence_proto::{Snapshot, State};
use async_std::fs;
use async_std::fs::{File, OpenOptions};
use async_std::path::Path;
use async_trait::async_trait;
use bytes::Bytes;
use futures::AsyncWriteExt;
use prost::Message;
use std::io::ErrorKind;
use std::time::Duration;
use tracing::{Instrument, debug, error, info, info_span};

const MAIN_STATE_PATH: &str = "main_state.pb.bin";
const SNAPSHOT_PATH: &str = "snapshot.pb.bin";
const LOG_PATH: &str = "log.pb.bin";

#[async_trait]
pub trait Persistence {
    // TODO(dino): Add an "append" for entries, that doesn't replace the file.
    // TODO(dino): Have all these methods return errors rather than "expect".
    async fn write_state(&self, term: i64, voted: &Option<Server>);
    async fn write_entries(&self, name: String, entries: &Vec<Entry>);
    async fn write_snapshot(&self, name: String, snapshot: &Bytes);
}

#[derive(Debug, Clone)]
pub enum PersistenceOptions {
    Directory(String),
    NoPersistenceForTesting,
}

pub async fn new(
    options: PersistenceOptions,
) -> Result<Box<dyn Persistence + Send>, PersistenceError> {
    match options {
        PersistenceOptions::Directory(directory_path) => {
            FilePersistence::new(directory_path.as_str()).await
        }
        PersistenceOptions::NoPersistenceForTesting => Ok(Box::new(NoopPersistence {})),
    }
}

#[derive(Debug, Clone)]
pub struct PersistenceError {
    message: String,
}

impl PersistenceError {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}
struct NoopPersistence {}

#[async_trait]
impl Persistence for NoopPersistence {
    async fn write_state(&self, _: i64, _: &Option<Server>) {}
    async fn write_entries(&self, _: String, _: &Vec<Entry>) {}
    async fn write_snapshot(&self, _: String, _: &Bytes) {}
}

struct FilePersistence {
    directory: String,
    //log: File,
}

impl FilePersistence {
    pub async fn new(directory: &str) -> Result<Box<dyn Persistence + Send>, PersistenceError> {
        create_dir_if_not_exists(directory).await?;
        let result = Box::new(FilePersistence {
            directory: directory.to_string(),
        });
        info!("Created file persistence backed by directory {}", directory);
        Ok(result)
    }

    async fn write_to_file(&self, filename: &str, data: &[u8]) -> Result<(), PersistenceError> {
        let path = Path::new(self.directory.as_str()).join(filename);

        let mut file = OpenOptions::new()
            .write(true)
            .open(path.clone())
            .await
            .map_err(|e| {
                PersistenceError::new(format!(
                    "Failed to open file {:?} : {}",
                    path.to_str(),
                    e.to_string()
                ))
            })?;

        file.write(data)
            .await
            .map(|_| ())
            .map_err(|e| {
                PersistenceError::new(format!(
                    "Failed to write to file {:?} : {}",
                    path.to_str(),
                    e.to_string()
                ))
            })
    }
}

#[async_trait]
impl Persistence for FilePersistence {
    async fn write_state(&self, term: i64, voted: &Option<Server>) {
        let output = State {
            term,
            voted_for: voted.clone(),
            entries_file: LOG_PATH.to_string(),
            snapshot_file: SNAPSHOT_PATH.to_string(),
        };
        self.write_to_file(MAIN_STATE_PATH, &output.encode_to_vec()).await.expect("Failed to write main state file");
    }

    async fn write_entries(&self, name: String, entries: &Vec<Entry>) {
    }

    async fn write_snapshot(&self, name: String, snapshot: &Bytes) {
        let output = Snapshot {
        };
    }
}

async fn create_dir_if_not_exists(directory: &str) -> Result<(), PersistenceError> {
    let dir_path = Path::new(directory);
    match fs::metadata(&dir_path).await {
        Ok(metadata) => {
            if metadata.is_dir() {
                Ok(())
            } else {
                Err(PersistenceError::new(format!(
                    "Path exists but is not a directory: {}",
                    directory
                )))
            }
        }
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                fs::create_dir_all(directory).await.map_err(|e| {
                    PersistenceError::new(format!("Failed to create directory {}", e.to_string()))
                })
            } else {
                Err(PersistenceError::new(format!(
                    "Unexpected filesystem error for {} : {}",
                    directory,
                    e.to_string()
                )))
            }
        }
    }
}
