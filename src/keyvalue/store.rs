extern crate bytes;
extern crate protobuf;

use crate::keyvalue::keyvalue_proto;
use crate::raft::{StateMachine, StateMachineResult};

use bytes::Bytes;
use std::collections::HashMap;

use self::bytes::Buf;
use self::protobuf::Message;
use keyvalue_proto::{Entry, Operation, Snapshot};

#[derive(Debug, Clone)]
struct StoreError {
    message: String,
}

impl StoreError {
    fn new(message: &str) -> Self {
        StoreError {
            message: String::from(message),
        }
    }
}

// A key-value store where both the key and the value type are just bytes.
pub trait Store {
    fn get(&self, key: &Bytes) -> Option<Bytes>;
    fn set(&mut self, key: Bytes, value: Bytes);
}

// A store implementation backed by a simple in-memory hash map.
pub struct MapStore {
    data: HashMap<Bytes, Bytes>,
}

impl MapStore {
    pub fn new() -> Self {
        MapStore {
            data: HashMap::new(),
        }
    }

    fn apply_operation(&mut self, operation: Operation) -> Result<(), StoreError> {
        if operation.has_set() {
            let set_op = operation.get_set();
            if !set_op.has_entry() {
                return Err(StoreError::new("No entry present in 'set' operation"));
            }

            let entry = set_op.get_entry();
            self.set(entry.get_key().to_bytes(), entry.get_value().to_bytes());
            return Ok(());
        }
        Err(StoreError::new("Unrecognized operation type"))
    }
}

impl Store for MapStore {
    fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.data.get(key).cloned()
    }

    fn set(&mut self, key: Bytes, value: Bytes) {
        self.data.insert(key, value);
    }
}

impl StateMachine for MapStore {
    fn apply(&mut self, payload: &Bytes) -> StateMachineResult {
        let parsed = protobuf::parse_from_bytes(payload.bytes());
        if parsed.is_err() {
            return Err(format!(
                "Failed to parse bytes: {:?}",
                parsed.err().unwrap()
            ));
        }
        let applied = self.apply_operation(parsed.unwrap());
        if applied.is_err() {
            return Err(format!(
                "Failed to apply operation: {:?}",
                applied.err().unwrap()
            ));
        }
        Ok(())
    }

    fn create_snapshot(&self) -> Bytes {
        let mut snapshot = Snapshot::new();
        for (k, v) in &self.data {
            let mut entry = Entry::new();
            entry.set_key(k.to_vec());
            entry.set_value(v.to_vec());
            snapshot.mut_entries().push(entry);
        }
        Bytes::from(snapshot.write_to_bytes().expect("serialization"))
    }

    fn load_snapshot(&mut self, snapshot: &Bytes) -> StateMachineResult {
        let parsed = protobuf::parse_from_bytes(snapshot.bytes());
        if parsed.is_err() {
            return Err(format!(
                "Failed to parse snapshot: {:?}",
                parsed.err().unwrap()
            ));
        }

        let contents: Snapshot = parsed.unwrap();
        self.data.clear();
        for entry in contents.get_entries() {
            self.data
                .insert(entry.get_key().to_bytes(), entry.get_value().to_bytes());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keyvalue_proto::{Entry, SetOperation};
    use protobuf::Message;

    fn make_entry(k: &Bytes, v: &Bytes) -> Entry {
        let mut result = Entry::new();
        result.set_key(k.to_vec());
        result.set_value(v.to_vec());
        result
    }

    fn make_set_op(k: &Bytes, v: &Bytes) -> Operation {
        let mut set_op = SetOperation::new();
        set_op.set_entry(make_entry(k, v));
        let mut op = Operation::new();
        op.set_set(set_op);
        op
    }

    #[test]
    fn test_map_store_get_set() {
        let mut store = MapStore::new();

        let k = Bytes::from("some-key");
        assert!(store.get(&k).is_none());

        let v1 = Bytes::from("value1");
        store.set(k.clone(), v1.clone());
        assert_eq!(v1, store.get(&k).unwrap());

        let v2 = Bytes::from("value2");
        store.set(k.clone(), v2.clone());
        assert_eq!(v2, store.get(&k).unwrap());
    }

    #[test]
    fn test_map_store_apply_happy() {
        let mut store = MapStore::new();

        let k = Bytes::from("some-key");
        let v = Bytes::from("some-value");

        let op = make_set_op(&k, &v);
        let serialized = Bytes::from(op.write_to_bytes().expect("serialize"));

        assert!(store.get(&k).is_none());
        store.apply(&serialized).expect("apply");
        assert_eq!(v, store.get(&k).unwrap());
    }

    #[test]
    fn test_map_store_apply_malformed() {
        let mut store = MapStore::new();

        let gibberish = Bytes::from("not an actual valid proto");
        assert!(store.apply(&gibberish).is_err());
    }

    #[test]
    fn test_map_store_snapshot() {
        let k1 = Bytes::from("key1");
        let v1 = Bytes::from("value1");
        let k2 = Bytes::from("key2");
        let v2 = Bytes::from("value2");

        let mut store = MapStore::new();
        store.set(k1.clone(), v1.clone());
        let snap = store.create_snapshot();

        let mut other_store = MapStore::new();
        other_store.set(k2.clone(), v2.clone());

        // Check that the value present in the snapshot is not in the store.
        assert!(other_store.get(&k1).is_none());

        other_store
            .load_snapshot(&snap)
            .expect("load should succeed");
        assert_eq!(other_store.get(&k1).unwrap(), &v1);
        assert!(other_store.get(&k2).is_none());
    }

    #[test]
    fn test_map_store_load_snapshot_malformed() {
        let mut store = MapStore::new();

        let gibberish = Bytes::from("not an actual valid proto");
        assert!(store.load_snapshot(&gibberish).is_err());
    }
}
