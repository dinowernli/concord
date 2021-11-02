use crate::raft::log::{ContainsResult, LogSlice};
use crate::raft::raft_proto::EntryId;
use crate::raft::StateMachine;
use async_std::sync::{Arc, Mutex};
use bytes::Bytes;
use futures::channel::oneshot::{channel, Receiver, Sender};
use log::{debug, info, warn};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use tonic::Status;

// Handles persistent storage for a Raft member, including a snapshot starting
// from the beginning of time up to some index, and running log of entries since
// the index included in the last snapshot.
//
// Also takes care of periodic compaction, updating listeners when the committed
// portion reaches as certain index, etc.
pub struct Store {
    pub log: LogSlice,
    state_machine: Arc<Mutex<dyn StateMachine + Send>>,
    snapshot: Option<LogSnapshot>,

    listener_uid: i64,
    listeners: BTreeSet<CommitListener>,

    committed: i64,
    applied: i64,

    compaction_threshold_bytes: i64,
    name: String,
}

impl Store {
    pub fn new(
        state_machine: Arc<Mutex<dyn StateMachine + Send>>,
        compaction_threshold_bytes: i64,
        name: &str,
    ) -> Self {
        Self {
            log: LogSlice::initial(),
            state_machine,
            snapshot: None,
            listener_uid: 0,
            listeners: BTreeSet::new(),
            committed: -1,
            applied: -1,
            compaction_threshold_bytes,
            name: name.to_string(),
        }
    }

    // Returns the index up to (and including) which the corresponding entries are
    // considered committed.
    pub fn committed_index(&self) -> i64 {
        self.committed
    }

    // Compacts logs entries into a new snapshot if necessary.
    pub async fn try_compact(&mut self) {
        if self.log.size_bytes() > self.compaction_threshold_bytes {
            let applied_index = self.applied;
            let latest_id = self.log.id_at(applied_index);
            let snap = LogSnapshot {
                snapshot: self.state_machine.lock().await.create_snapshot(),
                last: latest_id.clone(),
            };
            self.snapshot = Some(snap);

            // Note that we prefer not to clear the log slice entirely
            // because although losing uncommitted entries is safe, the
            // leader doing this could result in failed commit operations
            // sent to the leader.
            self.log.prune_until(&latest_id);

            info!(
                "[{}] Compacted log with snapshot up to (including) index {}",
                self.name, applied_index
            );
        }
    }

    // Marks the stored entries up to (and including) the supplied index as committed,
    // informing any listeners if necessary, applying changes to the state machine, etc.
    pub async fn commit_to(&mut self, new_commit_index: i64) {
        if new_commit_index <= self.committed {
            // Nothing to do here, already committed past the supplied index.
            return;
        }

        // This has a built-in assertion that the entry is present in the log.
        self.log.id_at(new_commit_index);

        let old_commit_index = self.committed;
        self.committed = new_commit_index;
        if new_commit_index > old_commit_index {
            debug!(
                "[{}] Updated committed index from {} to {}",
                &self.name, old_commit_index, self.committed
            );
        }

        self.apply_committed().await;
        self.resolve_listeners();
    }

    // Registers a listener for the supplied index.
    pub fn add_listener(&mut self, index: i64) -> Receiver<EntryId> {
        let (sender, receiver) = channel::<EntryId>();
        self.listeners.insert(CommitListener {
            index,
            sender,
            uid: self.listener_uid,
        });
        self.listener_uid = self.listener_uid + 1;
        receiver
    }

    // Resets the state of this store to the supplied snapshot.
    pub async fn install_snapshot(&mut self, snapshot: Bytes, last: EntryId) -> Status {
        // Update the state machine.
        let mut state_machine = self.state_machine.lock().await;
        match state_machine.load_snapshot(&snapshot) {
            Ok(_) => (),
            Err(message) => {
                return Status::internal(format!(
                    "[{}] Failed to load snapshot: [{:?}]",
                    &self.name, message
                ));
            }
        }

        // Update the log.
        let contains = self.log.contains(&last);
        match contains {
            ContainsResult::ABSENT => {
                // Common case, snapshot from the future. Just clear the log.
                self.log = LogSlice::new(last.clone());
            }
            ContainsResult::PRESENT => {
                // Entries that came after the latest snapshot entry might still be valid.
                self.log.prune_until(&last);
            }
            ContainsResult::COMPACTED => {
                warn!(
                    "[{}] Got snapshot for already compacted index {}",
                    &self.name, last.index,
                );
            }
        }

        self.applied = last.index;
        self.committed = last.index;
        self.snapshot = Some(LogSnapshot { last, snapshot });

        Status::ok("")
    }

    pub fn get_latest_snapshot(&self) -> Option<LogSnapshot> {
        self.snapshot.clone()
    }

    // Called to apply any committed values that haven't been applied to the
    // state machine. This method is always safe to call, on leaders and followers.
    async fn apply_committed(&mut self) {
        while self.applied < self.committed {
            self.applied = self.applied + 1;
            let entry = self.log.entry_at(self.applied);
            let entry_id = entry.id.expect("id").clone();

            let result = self
                .state_machine
                .lock()
                .await
                .apply(&Bytes::from(entry.payload));
            match result {
                Ok(()) => {
                    info!(
                        "[{}] Applied entry: {}",
                        &self.name,
                        entry_id_key(&entry_id)
                    );
                }
                Err(msg) => {
                    warn!(
                        "[{}] Failed to apply {}: {}",
                        &self.name,
                        entry_id_key(&entry_id),
                        msg,
                    );
                }
            }
        }
    }

    // Tries to resolve the promises for listeners waiting for commits.
    fn resolve_listeners(&mut self) {
        while self.listeners.first().is_some()
            && self.listeners.first().unwrap().index <= self.committed
        {
            let next = self.listeners.pop_first().expect("get first");
            let index = next.index;
            if !self.log.is_index_compacted(index) {
                next.sender
                    .send(self.log.id_at(index))
                    .map_err(|_| warn!("Listener for commit {} no longer listening", index))
                    .ok();
            } else {
                // Do nothing, we just let the sender go out of scope, which will notify the
                // receiver of the cancellation
            }
        }
    }
}

// Represents a snapshot of the state machine after applying a complete prefix
// of entries since the beginning of time.
#[derive(Clone)]
pub struct LogSnapshot {
    // The id of the latest entry included in the snapshot.
    pub last: EntryId,

    // The snapshot bytes as produced by the state machine.
    pub snapshot: Bytes,
}

// Each instance represents an ongoing commit operation waiting for the committed
// index to reach the index of their tentative new entry.
#[derive(Debug)]
struct CommitListener {
    // The index the listener would like to be notified about.
    index: i64,

    // Used to send the resulting entry id to the listener.
    sender: Sender<EntryId>,

    // Used to disambiguate between structs for the same index.
    uid: i64,
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

fn entry_id_key(entry_id: &EntryId) -> String {
    format!("(term={},id={})", entry_id.term, entry_id.index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::testing::FakeStateMachine;

    const COMPACTION_THRESHOLD_BYTES: i64 = 5000;

    fn make_store() -> Store {
        Store::new(
            Arc::new(Mutex::new(FakeStateMachine::new())),
            COMPACTION_THRESHOLD_BYTES,
            "testing-store",
        )
    }

    #[test]
    fn test_initial() {
        let store = make_store();
        assert_eq!(store.committed, -1);
        assert_eq!(store.applied, -1);
    }
}
