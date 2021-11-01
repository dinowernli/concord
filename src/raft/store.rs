use crate::raft::log::LogSlice;
use crate::raft::raft_proto::EntryId;
use crate::raft::StateMachine;
use async_std::sync::{Arc, Mutex};
use bytes::Bytes;
use futures::channel::oneshot::{channel, Receiver, Sender};
use std::cmp::Ordering;
use std::collections::BTreeSet;

// Handles persistent storage for a Raft member, including a snapshot starting
// from the beginning of time up to some index, and running log of entries since
// the index included in the last snapshot.
//
// Also takes care of periodic compaction, updating listeners when the committed
// portion reaches as certain index, etc.
pub struct Store {
    pub log: LogSlice,
    pub state_machine: Arc<Mutex<dyn StateMachine + Send>>,
    pub snapshot: Option<LogSnapshot>,
    pub listener_uid: i64,
    pub listeners: BTreeSet<CommitListener>,
}

impl Store {
    pub fn new(state_machine: Arc<Mutex<dyn StateMachine + Send>>) -> Self {
        Self {
            log: LogSlice::initial(),
            state_machine,
            snapshot: None,
            listener_uid: 0,
            listeners: BTreeSet::new(),
        }
    }
}

// Represents a snapshot of the state machine after applying a complete prefix
// of entries since the beginning of time.
pub struct LogSnapshot {
    // The id of the latest entry included in the snapshot.
    pub last: EntryId,

    // The snapshot bytes as produced by the state machine.
    pub snapshot: Bytes,
}

// Each instance represents an ongoing commit operation waiting for the committed
// index to reach the index of their tentative new entry.
#[derive(Debug)]
pub struct CommitListener {
    // The index the listener would like to be notified about.
    pub index: i64,

    // Used to send the resulting entry id to the listener.
    pub sender: Sender<EntryId>,

    // Used to disambiguate between structs for the same index.
    pub uid: i64,
}

impl Eq for CommitListener {}

impl PartialEq<Self> for CommitListener {
    fn eq(&self, other: &Self) -> bool {
        (self.index, self.uid).eq(&(other.index, other.uid))
    }
}

impl PartialOrd<Self> for CommitListener {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.index, self.uid).partial_cmp(&(other.index, other.uid))
    }
}

impl Ord for CommitListener {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.index, self.uid).cmp(&(other.index, other.uid))
    }
}
