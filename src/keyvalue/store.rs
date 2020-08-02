extern crate bytes;
extern crate protobuf;

use crate::keyvalue::keyvalue_proto;
use crate::raft::StateMachine;

use bytes::Bytes;
use log::warn;
use std::collections::HashMap;

use self::bytes::Buf;
use keyvalue_proto::Operation;

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
            self.set(set_op.get_key().to_bytes(), set_op.get_value().to_bytes());
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
    fn apply(&mut self, payload: &Bytes) {
        let result: Result<(), StoreError> = protobuf::parse_from_bytes(payload.bytes())
            .map(|operation| self.apply_operation(operation))
            .map_err(|err| StoreError::new(err.to_string().as_str()))
            .flatten();
        match result {
            Ok(()) => (),
            Err(msg) => warn!("Could not parse bytes as Operation proto: {:?}", msg),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keyvalue_proto::SetOperation;
    use protobuf::Message;

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

        let mut set = SetOperation::new();
        set.set_key(k.to_vec());
        set.set_value(v.to_vec());

        let mut operation = Operation::new();
        operation.set_set(set);

        let serialized = Bytes::from(operation.write_to_bytes().expect("serialize"));

        assert!(store.get(&k).is_none());
        store.apply(&serialized);
        assert_eq!(v, store.get(&k).unwrap());
    }

    #[test]
    fn test_map_store_apply_malformed() {
        let mut store = MapStore::new();

        let gibberish = Bytes::from("not an actual valid proto");
        store.apply(&gibberish);
    }
}
