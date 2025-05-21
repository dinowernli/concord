use crate::raft::raft_common_proto::{Entry, EntryId, Server};
use crate::raft::raft_persistence_proto::{Snapshot, State};
use crate::raft::store::LogSnapshot;
use async_std::fs;
use async_std::fs::OpenOptions;
use async_std::io::Cursor;
use async_std::path::Path;
use async_trait::async_trait;
use bytes::Bytes;
use futures::AsyncWriteExt;
use prost::Message;
use std::io::ErrorKind;
use tracing::info;

const MAIN_STATE_PATH: &str = "main_state.pb.bin";
const SNAPSHOT_PATH: &str = "snapshot.pb.bin";
const LOG_PATH: &str = "log.pb.bin";

#[derive(Debug, Clone)]
pub struct PersistentState {
    pub term: i64,
    pub voted_for: Option<Server>,
    pub snapshot: LogSnapshot,
    pub entries: Vec<Entry>,
}

#[async_trait]
pub trait Persistence {
    async fn write_state(&self, term: i64, voted: &Option<Server>) -> Result<(), PersistenceError>;
    async fn write_entries(&self, entries: &Vec<Entry>) -> Result<(), PersistenceError>;
    async fn append_entries(&self, entries: &Vec<Entry>) -> Result<(), PersistenceError>;
    async fn write_snapshot(
        &self,
        snapshot: &Bytes,
        last: &EntryId,
    ) -> Result<(), PersistenceError>;

    async fn read(&self) -> Result<Option<PersistentState>, PersistenceError>;
}

#[derive(Debug, Clone)]
pub enum PersistenceOptions {
    FilePersistence(FilePersistenceOptions),

    #[allow(dead_code)] // Only used for testing.
    NoPersistenceForTesting,
}

#[derive(Debug, Clone)]
pub struct FilePersistenceOptions {
    // A directory path in the file system to use for persistence. When provided,
    // the file persistence instance will create the directory if necessary, and
    // only ever touch files in this directory.
    pub directory: String,

    // If true, wipe any existing persisted state on creation.
    pub wipe: bool,
}

pub async fn new(
    options: PersistenceOptions,
) -> Result<Box<dyn Persistence + Send>, PersistenceError> {
    match options {
        PersistenceOptions::FilePersistence(file_persistence_options) => {
            let file_persistence = FilePersistence::new(file_persistence_options).await?;
            let persistence: Box<dyn Persistence + Send> = file_persistence;
            Ok(persistence)
        }
        PersistenceOptions::NoPersistenceForTesting => Ok(Box::new(NoopPersistence {})),
    }
}

#[derive(Debug, Clone)]
pub struct PersistenceError {
    _message: String,
}

impl PersistenceError {
    pub fn new(message: String) -> Self {
        Self { _message: message }
    }
}
struct NoopPersistence {}

#[async_trait]
impl Persistence for NoopPersistence {
    async fn write_state(&self, _: i64, _: &Option<Server>) -> Result<(), PersistenceError> {
        Ok(())
    }
    async fn write_entries(&self, _: &Vec<Entry>) -> Result<(), PersistenceError> {
        Ok(())
    }

    async fn append_entries(&self, entries: &Vec<Entry>) -> Result<(), PersistenceError> {
        todo!()
    }

    async fn write_snapshot(&self, _: &Bytes, _: &EntryId) -> Result<(), PersistenceError> {
        Ok(())
    }

    async fn read(&self) -> Result<Option<PersistentState>, PersistenceError> {
        Ok(None)
    }
}

struct FilePersistence {
    directory: String,
}

#[derive(Debug, Clone, PartialEq)]
enum WriteMode {
    Replace,
    Append,
}

impl FilePersistence {
    pub async fn new(
        options: FilePersistenceOptions,
    ) -> Result<Box<FilePersistence>, PersistenceError> {
        let directory = options.directory.clone();
        create_dir_if_not_exists(directory.as_str()).await?;

        let result = Box::new(FilePersistence {
            directory: directory.to_string(),
        });
        info!("Created file persistence backed by directory {}", directory);

        if options.wipe {
            result.wipe().await?;
        }

        Ok(result)
    }
    
    async fn read_entries(&self) -> Result<Vec<Entry>, PersistenceError> {
        let path = Path::new(&self.directory).join(LOG_PATH);
        let data = async_std::fs::read(path).await.map_err(|e| {
            PersistenceError::new(format!("Failed to read log file: {}", e.to_string()))
        })?;
        decode_length_delimited_entries(&data)
    }

    async fn read_snapshot(&self) -> Result<(Bytes, EntryId), PersistenceError> {
        let path = Path::new(&self.directory).join(SNAPSHOT_PATH);
        let data = fs::read(path)
            .await
            .map_err(|e| PersistenceError::new(format!("Failed to read snapshot file: {}", e)))?;
        let snapshot = Snapshot::decode(&*data)
            .map_err(|e| PersistenceError::new(format!("Failed to decode snapshot: {}", e)))?;
        let last = snapshot
            .last
            .ok_or_else(|| PersistenceError::new("Missing last EntryId in snapshot".to_string()))?;
        Ok((Bytes::from(snapshot.data), last))
    }

    async fn read_state(&self) -> Result<(i64, Option<Server>), PersistenceError> {
        let path = Path::new(&self.directory).join(MAIN_STATE_PATH);
        let data = fs::read(path)
            .await
            .map_err(|e| PersistenceError::new(format!("Failed to read state file: {}", e)))?;
        let state = State::decode(&*data)
            .map_err(|e| PersistenceError::new(format!("Failed to decode state: {}", e)))?;
        Ok((state.term, state.voted_for))
    }

    async fn write_to_file(
        &self,
        filename: &str,
        data: &[u8],
        mode: WriteMode,
    ) -> Result<(), PersistenceError> {
        let path = Path::new(self.directory.as_str()).join(filename);

        let mut file = OpenOptions::new()
            .write(true)
            .create(true) // Create if doesn't exist.
            .append(mode == WriteMode::Append)
            .truncate(mode == WriteMode::Replace)
            .open(path.clone())
            .await
            .map_err(|e| {
                PersistenceError::new(format!(
                    "Failed to open file {:?} : {}",
                    path.to_str(),
                    e.to_string()
                ))
            })?;

        file.write(data).await.map(|_| ()).map_err(|e| {
            PersistenceError::new(format!(
                "Failed to write to file {:?} : {}",
                path.to_str(),
                e.to_string()
            ))
        })
    }

    // Deletes the specified file from the filesystem directory.
    async fn wipe_file(&self, filename: &str) -> Result<(), PersistenceError> {
        let path = Path::new(&self.directory).join(filename);
        match async_std::fs::remove_file(path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()), // Already deleted.
            Err(e) => Err(PersistenceError::new(format!(
                "Failed to delete file {}: {}",
                filename, e
            ))),
        }
    }

    // Deletes all persisted files from the filesystem directory.
    async fn wipe(&self) -> Result<(), PersistenceError> {
        self.wipe_file(SNAPSHOT_PATH).await?;
        self.wipe_file(LOG_PATH).await?;
        self.wipe_file(MAIN_STATE_PATH).await?;
        Ok(())
    }
}

#[async_trait]
impl Persistence for FilePersistence {
    async fn write_state(&self, term: i64, voted: &Option<Server>) -> Result<(), PersistenceError> {
        let output = State {
            term,
            voted_for: voted.clone(),
        };
        self.write_to_file(MAIN_STATE_PATH, &output.encode_to_vec(), WriteMode::Replace)
            .await
    }

    async fn write_entries(&self, entries: &Vec<Entry>) -> Result<(), PersistenceError> {
        let output = encode_length_delimited(entries).await?;
        self.write_to_file(LOG_PATH, &output, WriteMode::Replace)
            .await
    }

    async fn append_entries(&self, entries: &Vec<Entry>) -> Result<(), PersistenceError> {
        let output = encode_length_delimited(entries).await?;
        self.write_to_file(LOG_PATH, &output, WriteMode::Append)
            .await
    }

    async fn write_snapshot(&self, data: &Bytes, last: &EntryId) -> Result<(), PersistenceError> {
        let output = Snapshot {
            data: data.to_vec(),
            last: Some(last.clone()),
        };
        self.write_to_file(SNAPSHOT_PATH, &output.encode_to_vec(), WriteMode::Replace)
            .await
    }

    async fn read(&self) -> Result<Option<PersistentState>, PersistenceError> {
        let main_exists = Path::new(&self.directory)
            .join(MAIN_STATE_PATH)
            .exists()
            .await;
        let log_exists = Path::new(&self.directory).join(LOG_PATH).exists().await;
        let snapshot_exists = Path::new(&self.directory)
            .join(SNAPSHOT_PATH)
            .exists()
            .await;

        // Check for the empty state where none of the files are present.
        if !main_exists && !snapshot_exists && !log_exists {
            return Ok(None);
        }

        // If any is present, we need all three.
        if !main_exists || !snapshot_exists || !log_exists {
            return Err(PersistenceError::new(format!(
                "Need all three files, but got: [main_exists:{},snapshot_exists:{},log_exists:{}]",
                main_exists, snapshot_exists, log_exists
            )));
        }

        // Now we can read all three and return the result.
        let (term, voted_for) = self.read_state().await?;

        let (snapshot_bytes, last) = self.read_snapshot().await?;
        let entries = self.read_entries().await?;

        // TODO(dino): Perform some extra validation on the contents (e.g. index matching)
        Ok(Some(PersistentState {
            term,
            voted_for,
            snapshot: LogSnapshot {
                last,
                snapshot: snapshot_bytes,
            },
            entries,
        }))
    }
}

async fn encode_length_delimited(entries: &Vec<Entry>) -> Result<Vec<u8>, PersistenceError> {
    let mut buffer = Cursor::new(Vec::new());
    for entry in entries {
        let mut entry_buf = Vec::new();
        entry
            .encode_length_delimited(&mut entry_buf)
            .map_err(|e| PersistenceError::new(format!("Unable to encode entry: {}", e)))?;

        // Write to the in-memory buffer
        buffer
            .write_all(&entry_buf)
            .await
            .map_err(|e| PersistenceError::new(format!("Unable to write to buffer: {}", e)))?;
    }
    Ok(buffer.into_inner())
}

fn decode_length_delimited_entries(data: &[u8]) -> Result<Vec<Entry>, PersistenceError> {
    let mut entries = Vec::new();
    let mut buf = &data[..];

    while !buf.is_empty() {
        match Entry::decode_length_delimited(&mut buf) {
            Ok(entry) => entries.push(entry),
            Err(e) => {
                return Err(PersistenceError::new(format!(
                    "Failed to decode entry: {}",
                    e
                )));
            }
        }
    }

    Ok(entries)
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

#[cfg(test)]
mod tests {
    use super::*;
    
    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::raft::raft_common_proto::entry::Data;
        use crate::raft::raft_common_proto::{Entry, EntryId, Server};
        use tempfile::TempDir;

        async fn create_persistence(directory: &str) -> Result<Box<FilePersistence>, PersistenceError> {
            let options = FilePersistenceOptions {
                directory: directory.to_string(),
                wipe: false,
            };
            FilePersistence::new(options).await
        }

        #[tokio::test]
        async fn test_write_and_read_entries() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();
            let persistence = create_persistence(&dir_str).await.unwrap();

            let entries = sample_entries();
            persistence
                .write_entries(&entries)
                .await
                .expect("Failed to write entries");

            let read_entries = persistence
                .read_entries()
                .await
                .expect("Failed to read entries");

            assert_eq!(entries, read_entries);
        }

        #[tokio::test]
        async fn test_encode_decode_length_delimited_entries() {
            let entries = sample_entries();
            // Encode
            let encoded = encode_length_delimited(&entries)
                .await
                .expect("Encoding failed");
            // Decode
            let decoded = decode_length_delimited_entries(&encoded).expect("Decoding failed");
            assert_eq!(entries, decoded);
        }

        #[tokio::test]
        async fn test_write_and_read_snapshot() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();
            let persistence = create_persistence(&dir_str).await.unwrap();

            let payload = Bytes::from("snapshot_data_here");
            let id = EntryId { term: 5, index: 42 };

            persistence.write_snapshot(&payload, &id).await.unwrap();
            let (read_data, read_id) = persistence.read_snapshot().await.unwrap();

            assert_eq!(payload, read_data);
            assert_eq!(id, read_id);
        }

        #[tokio::test]
        async fn test_write_and_read_state() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();
            let persistence = create_persistence(&dir_str).await.unwrap();

            let term = 9;
            let voted = Some(Server {
                name: "s1".to_string(),
                host: "addr:1234".to_string(),
                port: 1234,
            });

            persistence.write_state(term, &voted).await.unwrap();
            let (read_term, read_voted) = persistence.read_state().await.unwrap();

            assert_eq!(term, read_term);
            assert_eq!(voted, read_voted);
        }

        #[tokio::test]
        async fn test_append_entries() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();
            let persistence = create_persistence(&dir_str).await.unwrap();

            let first_batch = vec![sample_entry(1, 1, "one"), sample_entry(1, 2, "two")];
            let second_batch = vec![sample_entry(1, 3, "three"), sample_entry(1, 4, "four")];

            persistence.write_entries(&first_batch).await.unwrap(); // Overwrite
            persistence.append_entries(&second_batch).await.unwrap(); // Append

            let all_entries = persistence.read_entries().await.unwrap();
            let expected = [first_batch, second_batch].concat();
            assert_eq!(all_entries, expected);
        }

        #[tokio::test]
        async fn test_append_multiple_times() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();
            let persistence = create_persistence(&dir_str).await.unwrap();

            let batch1 = vec![sample_entry(1, 1, "a")];
            let batch2 = vec![sample_entry(1, 2, "b")];
            let batch3 = vec![sample_entry(1, 3, "c")];

            persistence.write_entries(&batch1).await.unwrap(); // Overwrite
            persistence.append_entries(&batch2).await.unwrap(); // Append
            persistence.append_entries(&batch3).await.unwrap(); // Append

            let all = persistence.read_entries().await.unwrap();
            let expected = [batch1, batch2, batch3].concat();
            assert_eq!(all, expected);
        }

        #[tokio::test]
        async fn test_overwrite_after_append() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();
            let persistence = create_persistence(&dir_str).await.unwrap();

            let initial = vec![sample_entry(1, 1, "start")];
            let appended = vec![sample_entry(1, 2, "middle")];
            let overwrite = vec![sample_entry(2, 1, "new_start")];

            persistence.write_entries(&initial).await.unwrap(); // Overwrite
            persistence.append_entries(&appended).await.unwrap(); // Append
            persistence.write_entries(&overwrite).await.unwrap(); // Overwrite again

            let all = persistence.read_entries().await.unwrap();
            assert_eq!(all, overwrite); // Only overwrite should remain
        }

        #[tokio::test]
        async fn test_append_after_overwrite_sequence() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();
            let persistence = create_persistence(&dir_str).await.unwrap();

            let initial = vec![sample_entry(1, 1, "1")];
            let overwrite = vec![sample_entry(2, 1, "2")];
            let appended = vec![sample_entry(2, 2, "3")];

            persistence.write_entries(&initial).await.unwrap(); // Write [1]
            persistence.write_entries(&overwrite).await.unwrap(); // Overwrite [2]
            persistence.append_entries(&appended).await.unwrap(); // [2, 3]

            let all = persistence.read_entries().await.unwrap();
            let expected = [overwrite, appended].concat();
            assert_eq!(all, expected);
        }

        #[tokio::test]
        async fn test_write_then_empty_append() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();
            let persistence = create_persistence(&dir_str).await.unwrap();

            let initial = vec![sample_entry(1, 1, "first"), sample_entry(1, 2, "second")];
            let empty: Vec<Entry> = vec![];

            persistence.write_entries(&initial).await.unwrap(); // Write 2
            persistence.append_entries(&empty).await.unwrap(); // Append 0

            let all = persistence.read_entries().await.unwrap();
            assert_eq!(all, initial);
        }

        #[tokio::test]
        async fn test_multiple_writes_only_last_remains() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();
            let persistence = create_persistence(&dir_str).await.unwrap();

            let first = vec![sample_entry(1, 1, "one")];
            let second = vec![sample_entry(2, 1, "two")];
            let third = vec![sample_entry(3, 1, "three")];

            persistence.write_entries(&first).await.unwrap();
            persistence.write_entries(&second).await.unwrap();
            persistence.write_entries(&third).await.unwrap();

            let all = persistence.read_entries().await.unwrap();
            assert_eq!(all, third); // Only the final overwrite should persist
        }

        #[tokio::test]
        async fn test_full_read_returns_correct_state() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();
            let persistence = create_persistence(&dir_str).await.unwrap();

            let term = 42;
            let voted = Some(Server {
                name: "leader".to_string(),
                host: "localhost".to_string(),
                port: 8080,
            });
            let snapshot_data = Bytes::from("snapshot_data_blob");
            let last_entry_id = EntryId { term: 1, index: 99 };

            let entries = vec![sample_entry(1, 100, "a"), sample_entry(1, 101, "b")];

            // Write all required files
            persistence.write_state(term, &voted).await.unwrap();
            persistence
                .write_snapshot(&snapshot_data, &last_entry_id)
                .await
                .unwrap();
            persistence.write_entries(&entries).await.unwrap();

            // Now read them back using `read()`
            let state = persistence
                .read()
                .await
                .expect("read() failed")
                .expect("Expected Some");

            // Assert state matches what we wrote
            assert_eq!(state.term, term);
            assert_eq!(state.voted_for, voted);
            assert_eq!(state.entries, entries);
            assert_eq!(state.snapshot.last, last_entry_id);
            assert_eq!(state.snapshot.snapshot, snapshot_data);
        }

        #[tokio::test]
        async fn test_read_returns_none_when_no_files_exist() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();
            let persistence = create_persistence(&dir_str).await.unwrap();

            let result = persistence.read().await.unwrap();
            assert!(
                result.is_none(),
                "Expected read() to return None when no files exist"
            );
        }

        #[tokio::test]
        async fn test_read_fails_with_partial_files() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();
            let persistence = create_persistence(&dir_str).await.unwrap();

            // Write only one of the three
            let term = 10;
            let voted = Some(Server {
                name: "solo".to_string(),
                host: "127.0.0.1".to_string(),
                port: 7070,
            });
            persistence.write_state(term, &voted).await.unwrap();

            let err = persistence
                .read()
                .await
                .expect_err("Expected error due to missing files");
            assert!(err._message.contains("Need all three files"));
        }

        #[tokio::test]
        async fn test_read_fails_with_corrupted_main_state() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();
            let persistence = create_persistence(&dir_str).await.unwrap();

            // Corrupt main_state.pb.bin
            let state_path = Path::new(dir_str.as_str()).join(MAIN_STATE_PATH);
            fs::write(&state_path, b"not a valid protobuf")
                .await
                .unwrap();

            // Write valid snapshot and log files
            let snapshot = Bytes::from("valid snapshot");
            let last = EntryId { term: 1, index: 1 };
            let entries = vec![sample_entry(1, 1, "valid")];

            persistence.write_snapshot(&snapshot, &last).await.unwrap();
            persistence.write_entries(&entries).await.unwrap();

            let result = persistence.read().await;
            assert!(
                result.is_err(),
                "Expected read() to fail due to corrupted main_state"
            );
        }

        #[tokio::test]
        async fn test_wipe_removes_existing_state() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();
            let persistence = create_persistence(&dir_str).await.unwrap();

            // Only write two out of the three files.
            let snapshot = Bytes::from("snapshot_data");
            let last_id = EntryId { term: 1, index: 1 };
            persistence.write_entries(&sample_entries()).await.unwrap();
            persistence
                .write_snapshot(&snapshot, &last_id)
                .await
                .unwrap();

            // Now create persistence again, with wipe = true
            let wiped_options = FilePersistenceOptions {
                directory: dir_str.clone(),
                wipe: true,
            };
            let wiped_persistence = FilePersistence::new(wiped_options).await.unwrap();

            // After wipe, reading should return None
            let result = wiped_persistence.read().await.unwrap();
            assert!(result.is_none());
        }

        #[tokio::test]
        async fn test_wipe_on_fresh_directory() {
            let temp_dir = TempDir::new().unwrap();
            let dir_str = temp_dir.path().to_str().unwrap().to_string();

            // Create persistence with wipe=true on empty dir (should not fail)
            let options = FilePersistenceOptions {
                directory: dir_str.clone(),
                wipe: true,
            };

            let persistence = FilePersistence::new(options).await.unwrap();

            // Nothing was there, so read should return None
            let state = persistence.read().await.unwrap();
            assert!(state.is_none());
        }

        #[tokio::test]
        async fn test_wipe_does_not_delete_unrelated_files() {
            let temp_dir = TempDir::new().unwrap();
            let dir = temp_dir.path();
            let dir_str = dir.to_str().unwrap();

            // Create an unrelated file
            let unrelated_path = dir.join("unrelated.txt");
            async_std::fs::write(&unrelated_path, b"preserve this")
                .await
                .unwrap();
            assert!(fs::metadata(&unrelated_path).await.is_ok());

            let persistence = create_persistence(&dir_str).await.unwrap();

            // Create a regular Raft file
            let entries_path = dir.join(LOG_PATH);
            persistence.write_entries(&sample_entries()).await.unwrap();
            persistence.wipe().await.unwrap();

            assert!(fs::metadata(&unrelated_path).await.is_ok());
            assert!(!fs::metadata(&entries_path).await.is_ok());
        }

        fn sample_entries() -> Vec<Entry> {
            vec![
                sample_entry(1, 2, "first"),
                sample_entry(2, 3, "second"),
                sample_entry(3, 4, "third"),
            ]
        }

        fn sample_entry(term: i64, index: i64, data: &str) -> Entry {
            Entry {
                id: Some(EntryId { term, index }),
                data: Some(Data::Payload(data.to_string().encode_to_vec())),
            }
        }
    }
}
