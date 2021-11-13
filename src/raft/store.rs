use crate::raft::log::{ContainsResult, LogSlice};
use crate::raft::raft_proto::entry::Data;
use crate::raft::raft_proto::entry::Data::Config;
use crate::raft::raft_proto::{Entry, EntryId};
use crate::raft::StateMachine;
use async_std::sync::{Arc, Mutex};
use bytes::Bytes;
use futures::channel::oneshot::{channel, Receiver, Sender};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use tonic::Status;
use tracing::{debug, info, warn};

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
    config_info: ConfigInfo,

    listener_uid: i64,
    listeners: BTreeSet<CommitListener>,

    committed: i64,
    applied: i64,

    compaction_threshold_bytes: i64,
    name: String,
}

// Holds information about config struts in the store.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigInfo {
    // The latest appended entry containing a cluster config.
    pub latest: Option<Entry>,

    // Whether or not the latest entry has been committed.
    pub committed: bool,
}

impl ConfigInfo {
    fn empty() -> Self {
        ConfigInfo {
            latest: None,
            committed: false,
        }
    }
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
            config_info: ConfigInfo::empty(),

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

    // Returns a copy of the latest snapshot backing this store, if any.
    pub fn get_latest_snapshot(&self) -> Option<LogSnapshot> {
        self.snapshot.clone()
    }

    // Returns the index of the first entry not (yet) present in the store.
    pub fn next_index(&self) -> i64 {
        self.log.next_index()
    }

    // Applies the supplied data to the log (without necessarily committing it). Returns
    // the id for the created entry.
    pub fn append(&mut self, term: i64, data: Data) -> EntryId {
        // Make sure we only do the expensive "update_config_info" if the latest
        // entry is actually a config entry.
        let is_config = matches!(&data, Config(_));

        let entry_id = self.log.append(term, data);
        if is_config {
            self.update_config_info();
        }
        entry_id
    }

    // Adds the supplied entries to the end of the store. Any conflicting
    // entries are replaced. Any existing entries with indexes higher than the
    // supplied entries are pruned.
    pub fn append_all(&mut self, entries: &[Entry]) {
        let is_any_config = entries.iter().any(|e| matches!(e.data, Some(Config(_))));

        self.log.append_all(entries);

        if is_any_config {
            self.update_config_info();
        }
    }

    // Returns information about cluster config entries in the store.
    pub fn get_config_info(&self) -> ConfigInfo {
        self.config_info.clone()
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

        // Note that since we've only moved the committed index and not appended
        // any new entries, we only need to do a full "update_config_info", it
        // suffices to check whether we've committed the latest config yet.
        self.update_config_info_committed();

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

        // This makes sure that if a listener is added for an index that's
        // already been committed, we resolve it immediately.
        self.resolve_listeners();

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

    // Called to apply any committed values that haven't been applied to the
    // state machine. This method is always safe to call, on leaders and followers.
    async fn apply_committed(&mut self) {
        while self.applied < self.committed {
            self.applied = self.applied + 1;
            let entry = self.log.entry_at(self.applied);
            let entry_id = entry.id.expect("id").clone();

            if let Some(Data::Payload(bytes)) = entry.data {
                let result = self.state_machine.lock().await.apply(&Bytes::from(bytes));
                match result {
                    Ok(()) => {
                        debug!(entry=%entry_id_key(&entry_id), "applied");
                    }
                    Err(msg) => {
                        warn!(entry=%entry_id_key(&entry_id), "failed to apply: {}", msg);
                    }
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

    // Updates our cached information about the latest config entry in the store.
    fn update_config_info(&mut self) {
        self.config_info.latest = self.log.latest_config_entry();
        self.update_config_info_committed();
    }

    // Only updates the committed bit in the latest config info.
    fn update_config_info_committed(&mut self) {
        let entry = &self.config_info.latest;
        if entry.is_none() {
            return;
        }

        let index = entry.as_ref().unwrap().id.as_ref().expect("id").index;
        self.config_info.committed = self.committed >= index;
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
    use crate::raft::raft_proto::entry::Data::Payload;
    use crate::raft::testing::FakeStateMachine;
    use futures::FutureExt;

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
        assert_eq!(store.committed_index(), -1);
        assert_eq!(store.applied, -1);
        assert!(store.get_latest_snapshot().is_none());
        assert_eq!(store.next_index(), 0);
    }

    #[tokio::test]
    #[should_panic]
    async fn test_commit_to_bad_index() {
        let mut store = make_store();
        store.append(2, Payload(Vec::new()));

        // Attempt to "commit to" a value which hasn't yet been appended.
        store.commit_to(17).await;
    }

    #[tokio::test]
    async fn test_commit_to() {
        let mut store = make_store();
        let eid = store.append(2, Payload(Vec::new()));

        // Should succeed.
        store.commit_to(eid.index).await;

        // Should succeed (noop).
        store.commit_to(eid.index - 1).await;
    }

    #[tokio::test]
    async fn test_listener() {
        let mut store = make_store();
        let receiver = store.add_listener(2);

        store.append(67, Payload(Vec::new()));
        store.append(67, Payload(Vec::new()));
        store.append(68, Payload(Vec::new()));

        store.commit_to(2).await;
        let output = receiver.now_or_never();
        assert!(output.is_some());

        let result = output.unwrap().unwrap();
        assert_eq!(2, result.index);
        assert_eq!(68, result.term);
    }

    #[tokio::test]
    async fn test_listener_multi() {
        let mut store = make_store();
        let receiver1 = store.add_listener(0);
        let receiver2 = store.add_listener(1);
        let receiver3 = store.add_listener(0);

        store.append(67, Payload(Vec::new()));
        store.commit_to(0).await;

        assert!(receiver1.now_or_never().is_some());
        assert!(receiver2.now_or_never().is_none());
        assert!(receiver3.now_or_never().is_some());
    }

    #[tokio::test]
    async fn test_listener_past() {
        let mut store = make_store();

        store.append(67, Payload(Vec::new()));
        store.append(67, Payload(Vec::new()));
        store.commit_to(1).await;

        let receiver = store.add_listener(0);
        assert!(receiver.now_or_never().is_some());
    }

    #[tokio::test]
    async fn test_compaction() {
        let mut store = make_store();
        let eid = store.append(
            67,
            Payload(vec![0; 2 * COMPACTION_THRESHOLD_BYTES as usize]),
        );
        assert!(store.log.size_bytes() > COMPACTION_THRESHOLD_BYTES);

        store.commit_to(eid.index).await;
        store.try_compact().await;

        assert!(store.log.size_bytes() < COMPACTION_THRESHOLD_BYTES);
    }

    #[tokio::test]
    async fn test_snapshot() {
        let mut store = make_store();
        assert!(store.get_latest_snapshot().is_none());

        store
            .install_snapshot(
                Bytes::from(""),
                EntryId {
                    term: 17,
                    index: 22,
                },
            )
            .await;
        assert_eq!(22, store.committed_index());

        let snap = store.get_latest_snapshot();
        assert!(snap.is_some());
        let last = snap.unwrap().last;
        assert_eq!(17, last.term);
        assert_eq!(22, last.index);
    }
}
