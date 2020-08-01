extern crate bytes;
use crate::raft::StateMachine;

use bytes::Bytes;
use std::collections::HashMap;

// A key-value store where both the key and the value type are just bytes.
pub trait Store {
    fn get(&self, key: Bytes) -> Option<Bytes>;
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
    fn get(&self, _key: Bytes) -> Option<Bytes> {
        // TODO(dino): Implement.
        None
    }
}

impl StateMachine for MapStore {
    fn apply(&mut self, _payload: &[u8]) {
        // TODO(dino): Implement.
    }
}
