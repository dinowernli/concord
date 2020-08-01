extern crate bytes;
extern crate protobuf;

use crate::keyvalue::keyvalue_proto;
use crate::raft::StateMachine;

use bytes::Bytes;
use log::warn;
use std::collections::HashMap;

use self::bytes::Buf;
use self::protobuf::ProtobufResult;
use keyvalue_proto::Operation;

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
        let parsed: ProtobufResult<Operation> = protobuf::parse_from_bytes(payload.bytes());
        match parsed {
            Err(msg) => warn!("Could not parse bytes as Operation proto: {}", msg),
            Ok(operation) => {
                if operation.has_set() {
                    let set_op = operation.get_set();
                    self.set(set_op.get_key().to_bytes(), set_op.get_value().to_bytes());
                } else {
                    warn!("Skipped invalid operation of unrecognized type");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_store() {
        let mut store = MapStore::new();

        let k = Bytes::from("some-key");
        assert!(store.get(&k).is_none());

        let v1 = Bytes::from("value1");
        store.set(k.clone(), v1.clone());

        assert_eq!(v1, store.get(&k).unwrap());
    }
}
