extern crate bytes;
extern crate im;

use crate::keyvalue::keyvalue_proto;
use crate::keyvalue::keyvalue_proto::operation::Op::Set;
use crate::keyvalue::persistence::{FilePersistence, Persistence};
use crate::raft::{StateMachine, StateMachineResult};
use async_trait::async_trait;
use bytes::Bytes;
use im::HashMap;
use keyvalue_proto::{Entry, Operation, Snapshot};
use prost::Message;
use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct StoreError {
    _message: String,
}

impl StoreError {
    fn new(message: &str) -> Self {
        StoreError {
            _message: String::from(message),
        }
    }
}

// A versioned key-value store where both the key and the value type are bytes.
// Each modification to the store creates a new version, leaving older versions
// still accessible until trimmed.
#[async_trait]
pub trait Store {
    // Returns the latest value associated with the supplied key.
    fn get(&self, key: &Bytes) -> Option<Bytes>;

    // Returns the value associated with the supplied key at a specific version
    // of the store. Returns an error if the supplied version is not present in
    // the store (because it hasn't occurred yet, or has been compacted).
    fn get_at(&self, key: &Bytes, version: i64) -> Result<Option<Bytes>, StoreError>;

    // Updates the supplied (key, value) pair, creating a new version.
    async fn set(&mut self, key: Bytes, value: Bytes);

    // Returns the latest (highest) version present in this store.
    fn latest_version(&self) -> i64;
}

// A store implementation backed by a simple in-memory hash map.
pub struct MapStore {
    // Holds a contiguous sequence of versions. This list is never empty.
    versions: VecDeque<MapVersion>,

    // If present, we will make sure all operations are reflected in persistently.
    persistence: Option<Box<dyn Persistence + Send>>,
}

// Holds the data associated with a particular version of the store. The data
// member typically shares internal data nodes with other versions of the same
// store instance.
struct MapVersion {
    version: i64,
    data: HashMap<Bytes, Bytes>,
}

impl MapStore {
    pub async fn new(directory: Option<String>) -> Self {
        MapStore {
            versions: create_deque(MapVersion {
                version: 0,
                data: HashMap::new(),
            }),
            persistence: match directory {
                None => None,
                Some(dir) => Some(Box::new(FilePersistence::create(dir).await)),
            },
        }
    }

    pub async fn create_in_memory() -> Self {
        Self::new(None /* directory */).await
    }

    async fn apply_operation(&mut self, operation: Operation) -> Result<(), StoreError> {
        match operation.op {
            Some(Set(set)) => {
                if !set.entry.is_some() {
                    return Err(StoreError::new("No entry present in 'set' operation"));
                }
                let entry = set.entry.unwrap();
                self.set(Bytes::from(entry.key), Bytes::from(entry.value))
                    .await;
                Ok(())
            }
            _ => Err(StoreError::new("Unrecognized operation type")),
        }
    }

    fn latest_data(&self) -> &HashMap<Bytes, Bytes> {
        self.versions.back().unwrap().data.as_ref()
    }

    fn earliest_version(&self) -> i64 {
        self.versions.front().unwrap().version
    }

    fn num_versions(&self) -> usize {
        self.versions.len()
    }

    fn version_index(&self, version: i64) -> Result<usize, StoreError> {
        let min = self.versions.front().expect("non-empty").version;
        let max = self.versions.back().expect("non-empty").version;
        if version < min || version > max {
            return Err(StoreError::new(
                format!(
                    "Invalid version {}, expected in range [{}, {}]",
                    version, min, max
                )
                .as_str(),
            ));
        }
        Ok((version - min) as usize)
    }
}

#[async_trait]
impl Store for MapStore {
    fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.get_at(key, self.latest_version())
            .expect("valid version")
    }

    fn get_at(&self, key: &Bytes, version: i64) -> Result<Option<Bytes>, StoreError> {
        let index = self.version_index(version)?;
        Ok(self
            .versions
            .get(index)
            .expect("valid index")
            .data
            .get(key)
            .cloned())
    }

    async fn set(&mut self, key: Bytes, value: Bytes) {
        let latest = self.versions.back().unwrap();
        let new_version = latest.version + 1;
        let new_data = latest.data.update(key.clone(), value.clone());

        if self.persistence.as_ref().is_some() {
            let entry = Entry {
                key: key.to_vec(),
                value: value.to_vec(),
            };
            self.persistence.as_mut().unwrap().add(&entry).await;
        }

        self.versions.push_back(MapVersion {
            version: new_version,
            data: new_data,
        });
    }

    fn latest_version(&self) -> i64 {
        self.versions.back().unwrap().version
    }
}

#[async_trait]
impl StateMachine for MapStore {
    async fn apply(&mut self, payload: &Bytes) -> StateMachineResult {
        let parsed = Operation::decode(payload.to_owned());
        if parsed.is_err() {
            return Err(format!(
                "Failed to parse bytes: {:?}",
                parsed.err().unwrap()
            ));
        }
        let applied = self.apply_operation(parsed.unwrap()).await;
        if applied.is_err() {
            return Err(format!(
                "Failed to apply operation: {:?}",
                applied.err().unwrap()
            ));
        }
        Ok(())
    }

    // Returns a serialized snapshot proto containing all entries present in
    // the latest version of the store.
    fn create_snapshot(&self) -> Bytes {
        let mut snapshot = Snapshot {
            version: self.latest_version(),
            entries: vec![],
        };
        for (k, v) in self.latest_data() {
            snapshot.entries.push(Entry {
                key: k.to_vec(),
                value: v.to_vec(),
            });
        }
        Bytes::from(snapshot.encode_to_vec())
    }

    // Discards all versions present in the current store instance, replacing
    // them with the supplied version.
    fn load_snapshot(&mut self, snapshot: &Bytes) -> StateMachineResult {
        let parsed = Snapshot::decode(snapshot.to_owned());
        if parsed.is_err() {
            return Err(format!(
                "Failed to parse snapshot: {:?}",
                parsed.err().unwrap()
            ));
        }

        let contents: Snapshot = parsed.unwrap();

        let mut data: HashMap<Bytes, Bytes> = HashMap::new();
        for entry in contents.entries {
            data.insert(Bytes::from(entry.key), Bytes::from(entry.value));
        }

        self.versions = create_deque(MapVersion {
            version: contents.version,
            data,
        });
        Ok(())
    }
}

fn create_deque<T>(item: T) -> VecDeque<T> {
    let mut result = VecDeque::new();
    result.push_back(item);
    result
}

#[cfg(test)]
mod tests {
    use keyvalue_proto::SetOperation;

    use super::*;

    fn make_set_op(k: &Bytes, v: &Bytes) -> Operation {
        Operation {
            op: Some(Set(SetOperation {
                entry: Some(Entry {
                    key: k.to_vec(),
                    value: v.to_vec(),
                }),
            })),
        }
    }

    #[tokio::test]
    async fn test_map_store_get_set() {
        let mut store = MapStore::create_in_memory().await;

        let k = Bytes::from("some-key");
        assert!(store.get(&k).is_none());

        let v1 = Bytes::from("value1");
        store.set(k.clone(), v1.clone()).await;
        assert_eq!(v1, store.get(&k).unwrap());

        let v2 = Bytes::from("value2");
        store.set(k.clone(), v2.clone()).await;
        assert_eq!(v2, store.get(&k).unwrap());
    }

    #[tokio::test]
    async fn test_map_store_apply_happy() {
        let mut store = MapStore::create_in_memory().await;

        let k = Bytes::from("some-key");
        let v = Bytes::from("some-value");

        let op = make_set_op(&k, &v);
        let serialized = op.encode_to_vec();

        assert!(store.get(&k).is_none());
        store.apply(&Bytes::from(serialized)).await.expect("apply");
        assert_eq!(v, store.get(&k).unwrap());
    }

    #[tokio::test]
    async fn test_map_store_apply_malformed() {
        let mut store = MapStore::create_in_memory().await;

        let gibberish = Bytes::from("not an actual valid proto");
        assert!(store.apply(&gibberish).await.is_err());
    }

    #[tokio::test]
    async fn test_map_store_snapshot() {
        let k1 = Bytes::from("key1");
        let v1 = Bytes::from("value1");
        let k2 = Bytes::from("key2");
        let v2 = Bytes::from("value2");

        let mut store = MapStore::create_in_memory().await;
        store.set(k1.clone(), v1.clone()).await;
        assert_eq!(store.latest_version(), 1);

        let snap = store.create_snapshot();

        let mut other_store = MapStore::create_in_memory().await;
        other_store.set(k2.clone(), v2.clone()).await;
        other_store.set(k2.clone(), v2.clone()).await;
        other_store.set(k2.clone(), v2.clone()).await;
        assert_eq!(other_store.latest_version(), 3);

        // Check that the value present in the snapshot is not in the store.
        assert!(other_store.get(&k1).is_none());

        other_store
            .load_snapshot(&snap)
            .expect("load should succeed");
        assert_eq!(other_store.get(&k1).unwrap(), &v1);
        assert!(other_store.get(&k2).is_none());
        assert_eq!(other_store.latest_version(), 1);
    }

    #[tokio::test]
    async fn test_map_store_load_snapshot_malformed() {
        let mut store = MapStore::create_in_memory().await;

        let gibberish = Bytes::from("not an actual valid proto");
        assert!(store.load_snapshot(&gibberish).is_err());
    }

    #[tokio::test]
    async fn test_map_store_versions() {
        let k1 = Bytes::from("key1");
        let v1 = Bytes::from("bar");
        let v2 = Bytes::from("baz");
        let v3 = Bytes::from("fib");
        let mut store = MapStore::create_in_memory().await;
        assert_eq!(store.latest_version(), 0);

        store.set(k1.clone(), v1.clone()).await;
        assert_eq!(store.latest_version(), 1);

        store.set(k1.clone(), v2.clone()).await;
        assert_eq!(store.latest_version(), 2);

        store.set(k1.clone(), v3.clone()).await;
        assert_eq!(store.latest_version(), 3);

        // Check that the versions have been updated correctly.
        assert_eq!(store.get_at(&k1, 1).unwrap().unwrap(), &v1);
        assert_eq!(store.get_at(&k1, 2).unwrap().unwrap(), &v2);
        assert_eq!(store.get_at(&k1, 3).unwrap().unwrap(), &v3);
    }
}
