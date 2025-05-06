use crate::keyvalue::keyvalue_proto::Entry;
use async_std::fs;
use async_std::fs::{File, OpenOptions};
use async_std::io::{BufReader, ReadExt};
use async_std::path::Path;
use async_trait::async_trait;
use bytes::Bytes;
use futures::AsyncWriteExt;
use prost::Message;
use tracing::info;

#[async_trait]
pub trait Persistence {
    async fn add(&mut self, entry: &Entry);
}

pub struct FilePersistence {
    directory: String,

    log_path: String,
    log: File,
}

impl FilePersistence {
    pub async fn create(directory: String) -> FilePersistence {
        let meta = fs::metadata(&directory).await.expect("exists");
        assert!(meta.is_dir());
        let wal_path = Path::new(directory.as_str()).join("wal");
        info!("WAL path: {}", &wal_path.display());
        let wal_file = OpenOptions::new()
            .write(true)
            .append(true)
            .create_new(true)
            .open(wal_path.clone())
            .await
            .expect("open wal file");
        FilePersistence {
            directory,
            log: wal_file,
            log_path: wal_path.to_str().unwrap().to_string(),
        }
    }

    pub async fn read_wal(&self) -> u64 {
        let file = OpenOptions::new()
            .read(true)
            .open(self.log_path.clone())
            .await
            .expect("open wal file");

        let mut reader = BufReader::new(file);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read");

        let mut entries = Vec::new();
        let mut slice = &buf[..];

        while !slice.is_empty() {
            match Entry::decode_length_delimited(&mut slice) {
                Ok(entry) => entries.push(entry),
                Err(err) => {
                    eprintln!("Failed to decode Entry: {}", err);
                    break;
                }
            }
        }
        entries.len() as u64
    }
}

#[async_trait]
impl Persistence for FilePersistence {
    async fn add(&mut self, entry: &Entry) {
        let mut buf = vec![];
        entry.encode_length_delimited(&mut buf).expect("encode");
        self.log.write(&buf).await.expect("write");
        self.log.flush().await.expect("flush");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_tempfile::TempDir;
    #[tokio::test]
    async fn test_something() {
        let tmp_dir = TempDir::new().await.unwrap();
        let tmp_dir_path = tmp_dir.dir_path().to_str().unwrap().to_string();
        let mut persistence = FilePersistence::create(tmp_dir_path).await;

        let k1 = Bytes::from("key1");
        let v1 = Bytes::from("value1");
        let k2 = Bytes::from("key2");
        let v2 = Bytes::from("value2");

        persistence
            .add(&Entry {
                key: k1.to_vec(),
                value: v1.to_vec(),
            })
            .await;
        assert_eq!(persistence.read_wal().await, 1);

        persistence
            .add(&Entry {
                key: k2.to_vec(),
                value: v2.to_vec(),
            })
            .await;
        assert_eq!(persistence.read_wal().await, 2);
    }
}
