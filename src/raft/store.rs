use crate::raft::log::{ContainsResult, LogSlice};
use crate::raft::persistence::{Persistence, PersistenceOptions};
use crate::raft::raft_common_proto::entry::Data;
use crate::raft::raft_common_proto::entry::Data::Config;
use crate::raft::raft_common_proto::{Entry, EntryId, Server};
use crate::raft::{StateMachine, persistence};
use async_std::sync::{Arc, Mutex};
use bytes::Bytes;
use futures::channel::oneshot::{Receiver, Sender, channel};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use tonic::{Code, Status};
use tracing::{debug, info, warn};

// Handles persistent storage for a Raft member, including a snapshot starting
// from the beginning of time up to some index, and running log of entries since
// the index included in the last snapshot.
//
// Also takes care of periodic compaction, updating listeners when the committed
// portion reaches as certain index, etc.
pub struct Store {
    // Dependencies and constants.
    compaction_threshold_bytes: i64,
    name: String,
    state_machine: Arc<Mutex<dyn StateMachine + Send>>,
    persistence: Box<dyn Persistence + Send>,

    // Persistent raft state as defined in the paper.
    log: LogSlice,
    snapshot: LogSnapshot,
    term: i64,
    voted_for: Option<Server>,

    // Non-persistent. Just bookkeeping.
    // TODO(dino): DONOTMERGE - need to check if config info needs to be persisted (probably yes)
    // Note: etcd keeps the config state in the snapshot, but has different implementation where
    // config changes only take effect once applied (rather than once appended).
    committed: i64,
    applied: i64,
    config_info: ConfigInfo,

    listener_uid: i64,
    listeners: BTreeSet<CommitListener>,
}

// Holds information about cluster configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigInfo {
    // The latest appended entry containing a cluster config.
    pub latest: Option<Entry>,

    // Whether the latest entry has been committed.
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
    pub async fn new(
        persistence_options: PersistenceOptions,
        state_machine: Arc<Mutex<dyn StateMachine + Send>>,
        compaction_threshold_bytes: i64,
        name: &str,
    ) -> Self {
        let persistence = persistence::new(persistence_options)
            .await
            .expect("create persistence");

        let initial_snapshot_bytes = state_machine.lock().await.create_snapshot();
        let initial_snapshot = LogSnapshot::make_initial(initial_snapshot_bytes);
        let last = initial_snapshot.last.index;

        let mut result = Self {
            name: name.to_string(),
            compaction_threshold_bytes,
            state_machine,
            persistence,

            log: LogSlice::initial(),
            snapshot: initial_snapshot,
            term: 0,
            voted_for: None,

            // Non-persistent. Just bookkeeping.
            committed: last,
            applied: last,
            config_info: ConfigInfo::empty(),

            listener_uid: 0,
            listeners: BTreeSet::new(),
        };

        // Try restoring state from our persistence source. It's important that we do this
        // immediately before calling anything else on the result store, to avoid the store
        // clobbering the persisted state as part of other operations.
        result.restore_persisted().await;

        // TODO(dino): Make sure we initialize the cluster constituents correctly.

        result
    }

    // Reads state from the persistence instance and installs it in this store.
    async fn restore_persisted(&mut self) {
        if let Some(loaded) = self.persistence.read().await.expect("read persisted state") {
            let snap = loaded.snapshot;
            let last = snap.last.clone();

            self.voted_for = loaded.voted_for;
            self.term = loaded.term;
            self.log = LogSlice::new(last);

            let status = self.install(snap.snapshot, snap.last, loaded.entries).await;
            assert_eq!(status.code(), Code::Ok);
        }
    }

    // Returns the index up to (and including) which the corresponding entries are
    // considered committed.
    pub fn committed_index(&self) -> i64 {
        self.committed
    }

    // Returns a copy of the latest snapshot backing this store, if any.
    pub fn get_latest_snapshot(&self) -> LogSnapshot {
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

    pub fn term(&self) -> i64 {
        self.term
    }

    pub fn voted_for(&self) -> Option<Server> {
        self.voted_for.clone()
    }

    // Updates just the "voted_for" part of the persistent state.
    pub async fn update_voted_for(&mut self, voted_for: &Option<Server>) {
        let term = self.term;
        self.update_term_info(term, voted_for).await;
    }

    // Updates the term information in persistent state.
    pub async fn update_term_info(&mut self, term: i64, voted_for: &Option<Server>) {
        self.term = term;
        self.voted_for = voted_for.clone();

        // TODO(dino): Important to make sure we don't accept or serve any more RPCs if this fails
        self.persistence
            .write_state(self.term, &self.voted_for)
            .await
            .expect("write state");
    }

    async fn update_snapshot(&mut self, snapshot: LogSnapshot) {
        self.snapshot = snapshot.clone();

        // TODO(dino): Important to make sure we don't accept or serve any more RPCs if this fails
        self.persistence
            .write_snapshot(&snapshot.snapshot, &snapshot.last)
            .await
            .expect("write snapshot");
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
            self.update_snapshot(snap).await;

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

    // Returns true if the supplied index lies before the range of entries present
    // in our in-memory log (e.g., because it was compacted into a snapshot).
    pub fn is_index_compacted(&self, index: i64) -> bool {
        self.log.is_index_compacted(index)
    }

    // Returns the entry id for the entry at the supplied index. Must only be
    // called if the index is known to be present in memory.
    //
    // Note that for the entry immediately before the start of the log slice, the
    // entry id can be returned, but not the full entry.
    pub fn entry_id_at_index(&self, index: i64) -> EntryId {
        self.log.id_at(index)
    }

    // Returns the highest index known to exist (even if the entry is not held
    // in this instance). Returns -1 if there are no known entries at all.
    pub fn last_known_log_entry_id(&self) -> &EntryId {
        self.log.last_known_id()
    }

    // Returns true if the supplied last entry id is at least as up-to-date
    // as the slice of the log tracked by this instance.
    pub fn log_entry_is_up_to_date(&self, other_last: &EntryId) -> bool {
        self.log.is_up_to_date(other_last)
    }

    // Returns true if the supplied entry is present in this slice.
    pub fn log_contains(&self, query: &EntryId) -> ContainsResult {
        self.log.contains(query)
    }

    // Returns all entries strictly after the supplied id. Must only be called
    // if the supplied entry id is present in the slice.
    pub fn get_entries_after(&self, entry_id: &EntryId) -> Vec<Entry> {
        self.log.get_entries_after(entry_id)
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

    pub async fn install_snapshot(&mut self, snapshot: Bytes, last: EntryId) -> Status {
        // Just a snapshot without additional entries.
        let add_entries = Vec::new();
        self.install(snapshot, last, add_entries).await
    }

    // Resets the state of this store to the supplied snapshot and log entries.
    async fn install(&mut self, snapshot: Bytes, last: EntryId, add_entries: Vec<Entry>) -> Status {
        // Store the new snapshot, including writing it to persistence.
        self.update_snapshot(LogSnapshot {
            last,
            snapshot: snapshot.clone(),
        })
        .await;

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
                // This means we've already advanced past the end of this snapshot and
                // previously compacted the new last entry. Here too, we clear the log.
                warn!(
                    "[{}] Got snapshot for already compacted index {}",
                    &self.name, last.index,
                );
                self.log = LogSlice::new(last.clone());
            }
        }

        // Append any new entries provided.
        if !add_entries.is_empty() {
            self.append_all(&add_entries);
        }

        // TODO(dino): need to persist to disk - perhaps just do as part of append_all?

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

        // Update our volatile state.
        self.applied = last.index;
        self.committed = last.index;

        // TODO(dino) DONOTMERGE - do we need to call resolve_listeners() here to catch the case
        // where the snapshot took us to a commit higher than some waiting listener?

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
#[derive(Debug, Clone)]
pub struct LogSnapshot {
    // The id of the latest entry included in the snapshot.
    pub last: EntryId,

    // The snapshot bytes as produced by the state machine.
    pub snapshot: Bytes,
}

impl LogSnapshot {
    pub fn make_initial(initial_bytes: Bytes) -> Self {
        LogSnapshot {
            last: EntryId {
                term: -1,
                index: -1,
            },
            snapshot: initial_bytes,
        }
    }
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
    use crate::raft::persistence::PersistenceOptions;
    use crate::raft::raft_common_proto::entry::Data::Payload;
    use crate::raft::raft_common_proto::{ClusterConfig, Server};
    use crate::raft::testing::FakeStateMachine;
    use futures::FutureExt;

    const COMPACTION_THRESHOLD_BYTES: i64 = 5000;

    async fn make_store() -> Store {
        Store::new(
            PersistenceOptions::NoPersistenceForTesting,
            Arc::new(Mutex::new(FakeStateMachine::new())),
            COMPACTION_THRESHOLD_BYTES,
            "testing-store",
        )
        .await
    }

    #[tokio::test]
    async fn test_initial() {
        let store = make_store().await;
        assert_eq!(store.committed_index(), -1);
        assert_eq!(store.applied, -1);
        assert_eq!(store.next_index(), 0);
    }

    #[tokio::test]
    #[should_panic]
    async fn test_commit_to_bad_index() {
        let mut store = make_store().await;
        store.append(2, Payload(Vec::new()));

        // Attempt to "commit to" a value which hasn't yet been appended.
        store.commit_to(17).await;
    }

    #[tokio::test]
    async fn test_commit_to() {
        let mut store = make_store().await;
        let eid = store.append(2, Payload(Vec::new()));

        // Should succeed.
        store.commit_to(eid.index).await;

        // Should succeed (noop).
        store.commit_to(eid.index - 1).await;
    }

    #[tokio::test]
    async fn test_listener() {
        let mut store = make_store().await;
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
        let mut store = make_store().await;
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
        let mut store = make_store().await;

        store.append(67, Payload(Vec::new()));
        store.append(67, Payload(Vec::new()));
        store.commit_to(1).await;

        let receiver = store.add_listener(0);
        assert!(receiver.now_or_never().is_some());
    }

    #[tokio::test]
    async fn test_compaction() {
        let mut store = make_store().await;
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
        let mut store = make_store().await;
        assert_eq!(store.get_latest_snapshot().last.term, -1);
        assert_eq!(store.get_latest_snapshot().last.index, -1);

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
        assert_eq!(17, snap.last.term);
        assert_eq!(22, snap.last.index);
    }

    #[tokio::test]
    async fn test_config_info_append() {
        let mut store = make_store().await;
        assert!(store.config_info.latest.is_none());

        store.append(17, Payload(Vec::new()));
        assert!(store.config_info.latest.is_none());

        let config = ClusterConfig {
            voters: vec![],
            voters_next: vec![],
        };
        store.append(17, Config(config.clone()));
        assert!(!store.config_info.latest.is_none());
    }

    #[tokio::test]
    async fn test_config_info_append_all() {
        let mut store = make_store().await;
        assert!(store.get_config_info().latest.is_none());

        store.append_all(&vec![
            payload_entry(12, 0),
            payload_entry(12, 1),
            payload_entry(12, 2),
        ]);
        assert!(store.get_config_info().latest.is_none());

        let voters = 7;
        store.append_all(&vec![
            payload_entry(12, 3),
            config_entry(12, 4, voters),
            payload_entry(12, 5),
        ]);
        assert!(store.get_config_info().latest.is_some());
        match store.get_config_info().latest.unwrap().data.unwrap() {
            Config(config) => assert_eq!(voters, config.voters.len()),
            _ => panic!("bad entry contents"),
        }

        let voters = 5;
        store.append_all(&vec![
            config_entry(12, 6, 3),
            config_entry(12, 7, 3),
            config_entry(12, 8, voters),
        ]);
        assert!(store.get_config_info().latest.is_some());
        match store.get_config_info().latest.unwrap().data.unwrap() {
            Config(config) => assert_eq!(voters, config.voters.len()),
            _ => panic!("bad entry contents"),
        }
        assert!(!store.get_config_info().committed);

        store.commit_to(8).await;
        assert!(store.get_config_info().committed);
    }

    #[tokio::test]
    async fn test_todo() {
        panic!()
        // Need tests for:
        // - Make sure persistence is valid after creating store (including after some operations)
        // - Make sure persistence doesn't clobber any state as part of loading
        // - Make sure the tests above run with persistence on
        // - Make sure installing a snapshot doesn't result in hanging listeners
    }

    fn payload_entry(term: i64, index: i64) -> Entry {
        Entry {
            id: Some(EntryId { term, index }),
            data: Some(Payload(Vec::new())),
        }
    }

    fn config_entry(term: i64, index: i64, num_voters: usize) -> Entry {
        let mut voters = Vec::new();
        for p in 0..num_voters {
            voters.push(server("some-host", p as i32));
        }
        Entry {
            id: Some(EntryId { term, index }),
            data: Some(Config(ClusterConfig {
                voters,
                voters_next: Vec::new(),
            })),
        }
    }

    fn server(host: &str, port: i32) -> Server {
        Server {
            host: host.to_string(),
            port,
            name: String::new(),
        }
    }
}
