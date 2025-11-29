use crate::raft::StateMachine;
use crate::raft::diagnostics::ServerDiagnostics;
use crate::raft::error::{RaftError, RaftResult};
use crate::raft::log::LogSlice;
use crate::raft::raft_common_proto::entry::Data;
use crate::raft::raft_common_proto::entry::Data::Config;
use crate::raft::raft_common_proto::{ClusterConfig, Entry, EntryId, Server};
use async_std::sync::{Arc, Mutex};
use bytes::Bytes;
use futures::channel::oneshot::{Receiver, Sender, channel};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use tonic::Status;
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
    diagnostics: Option<Arc<Mutex<ServerDiagnostics>>>,

    // Persistent raft state as defined in the paper.
    log: LogSlice,
    snapshot: LogSnapshot,
    term: i64,
    voted_for: Option<Server>,

    // Non-persistent. Just bookkeeping.
    committed: i64,
    applied: i64,
    config_info: ConfigInfo,

    listener_uid: i64,
    listeners: BTreeSet<CommitListener>,
}

// Holds information about cluster configuration.
#[derive(Clone, PartialEq)]
pub struct ConfigInfo {
    // The latest appended entry containing a cluster config.
    pub latest_appended: Option<Entry>,

    // Whether the latest appended entry has been committed.
    committed: bool,

    // The latest applied entry containing a cluster config.
    latest_applied: ClusterConfig,
}

impl ConfigInfo {
    // Returns the latest config, and whether it has been committed.
    pub fn latest(&self) -> (ClusterConfig, bool) {
        if let Some(e) = self.latest_appended.clone() {
            if let Config(config) = e.data.unwrap() {
                return (config, self.committed);
            }
        }

        // Nothing appended since snapshot installation, just return
        // the applied config.
        (self.latest_applied.clone(), true)
    }

    #[cfg(test)]
    pub fn make_with_appended(entry: Entry, committed: bool) -> Self {
        Self {
            latest_appended: Some(entry),
            committed,

            // Irrelevant if the fields above are set (based on implementation of latest()).
            latest_applied: ClusterConfig {
                voters: vec![],
                voters_next: vec![],
            },
        }
    }
}

impl Store {
    pub fn new(
        state_machine: Arc<Mutex<dyn StateMachine + Send>>,
        snapshot: LogSnapshot,
        entries: Vec<Entry>,
        diagnostics: Option<Arc<Mutex<ServerDiagnostics>>>,
        compaction_threshold_bytes: i64,
        name: &str,
    ) -> RaftResult<Self> {
        let last = snapshot.last.clone();
        let index = last.index.clone();
        let latest_applied = snapshot.config.clone();
        let config_info = ConfigInfo {
            latest_appended: None,
            committed: false,
            latest_applied,
        };
        Ok(Self {
            name: name.to_string(),
            compaction_threshold_bytes,
            state_machine,
            diagnostics,

            log: LogSlice::new(last, entries).map_err(|e| {
                RaftError::Initialization(format!("Failed to create log slice: {}", e))
            })?,
            snapshot,
            term: 0,
            voted_for: None,

            // Non-persistent. Just bookkeeping.
            committed: index,
            applied: index,
            config_info,

            listener_uid: 0,
            listeners: BTreeSet::new(),
        })
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
    pub async fn append_all(&mut self, entries: &[Entry]) {
        // TODO: This is in preparation for a future change where the store will be async.
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
        // TODO: This is in preparation for a future change where the store will be async.
        self.term = term;
        self.voted_for = voted_for.clone();
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
            let latest_applied_config = self.config_info.latest_applied.clone();
            let snap = LogSnapshot {
                data: self.state_machine.lock().await.create_snapshot(),
                last: latest_id.clone(),
                config: latest_applied_config,
            };
            self.snapshot = snap;

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

    // Returns true if the supplied entry ID represents a conflict, i.e., if we
    // have an entry with the supplied index, and a different term.
    pub fn conflict(&self, id: &EntryId) -> bool {
        self.log.conflict(id)
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

    pub async fn install_snapshot(&mut self, snapshot: LogSnapshot) -> Status {
        // Just a snapshot without additional entries.
        let add_entries = Vec::new();
        self.install(snapshot, add_entries).await
    }

    // Resets the state of this store to the supplied snapshot and log entries.
    async fn install(&mut self, snapshot: LogSnapshot, add_entries: Vec<Entry>) -> Status {
        let last = snapshot.last.clone();
        let data = snapshot.data.clone();
        let cluster_config = snapshot.config.clone();

        // Check that the snapshot does indeed take us into the future.
        if last.index < self.applied {
            return Status::invalid_argument(format!(
                "[{}] Refused to install snapshot that would go back in time. Last applied index in snapshot is {}, our latest applied index is {}",
                &self.name, &last.index, &self.applied
            ));
        }

        // Update the state machine. Do this before the operations below so that we can fail the
        // incoming RPC cleanly in case this fails, as if the call had never happened.
        let state_machine = self.state_machine.clone();
        let mut locked_machine = state_machine.lock().await;
        match locked_machine.load_snapshot(&data) {
            Ok(_) => (),
            Err(message) => {
                return Status::internal(format!(
                    "[{}] Failed to load snapshot: [{:?}]",
                    &self.name, message
                ));
            }
        }

        // Store the new snapshot.
        self.snapshot = snapshot.clone();

        // Update the log. There is some subtlety in why this call is the right thing.
        //
        // The common case here is that the snapshot's last entry is greater than anything
        // our log slice knows about, in which case we clear the log slice entirely.
        //
        // It's also possible that the snapshot's latest entry is somewhere in the range
        // (our_snapshot.latest, our_log.max_index]. In this case:
        //  * If the exact entry is present, keep everything that comes after
        //  * If the index is present, but it's the wrong entry, also clear the slice
        self.log.prune_until(&last);

        // Append any new entries provided.
        if !add_entries.is_empty() {
            self.append_all(&add_entries).await;
        }

        // Update our volatile state.
        self.applied = last.index;
        self.committed = last.index;
        self.config_info = ConfigInfo {
            latest_appended: None,
            committed: false,
            latest_applied: cluster_config,
        };

        // Go through listeners and have them catch up to the current commit.
        self.resolve_listeners();

        Status::ok("")
    }

    // If diagnostics is enabled, report that the supplied entry has been applied
    // to the state machine.
    async fn report_apply(&self, entry_id: &EntryId, bytes: &Vec<u8>) {
        let diag = self.diagnostics.clone();
        if !diag.is_some() {
            return;
        }

        let digest = Self::make_digest(&bytes);
        let term = entry_id.term;
        let index = entry_id.index;
        diag.expect("option")
            .lock()
            .await
            .report_apply(term, index, digest);
    }

    // Called to apply any committed values that haven't been applied to the
    // state machine. This method is always safe to call, on leaders and followers.
    async fn apply_committed(&mut self) {
        while self.applied < self.committed {
            self.applied = self.applied + 1;
            let entry = self.log.entry_at(self.applied);
            let entry_id = entry.id.expect("id").clone();

            if let Some(data) = entry.data {
                match data {
                    Data::Payload(bytes) => {
                        // Report for debugging purposes.
                        self.report_apply(&entry_id, &bytes).await;

                        // Apply the entry in the state machine.
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
                    Config(config) => {
                        self.config_info.latest_applied = config;
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
        self.config_info.latest_appended = self.log.latest_config_entry();
        self.update_config_info_committed();
    }

    // Only updates the committed bit in the latest config info.
    fn update_config_info_committed(&mut self) {
        let entry = &self.config_info.latest_appended;
        if entry.is_none() {
            return;
        }

        let index = entry.as_ref().unwrap().id.as_ref().expect("id").index;
        self.config_info.committed = self.committed >= index;
    }

    fn make_digest(bytes: &Vec<u8>) -> u64 {
        let mut hasher = std::hash::DefaultHasher::new();
        bytes.hash(&mut hasher);
        hasher.finish()
    }
}

// Represents a snapshot of the store after applying a complete prefix
// of entries since the beginning of time.
#[derive(Clone)]
pub struct LogSnapshot {
    // The id of the latest entry included in the snapshot.
    pub last: EntryId,

    // A snapshot of the state machine.
    pub data: Bytes,

    // The cluster configuration present in the latest included entry
    // that contains a cluster configuration.
    pub config: ClusterConfig,
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
    use crate::raft::raft_common_proto::entry::Data::Payload;
    use crate::raft::raft_common_proto::{ClusterConfig, Server};
    use crate::raft::testing::FakeStateMachine;
    use futures::FutureExt;

    const COMPACTION_THRESHOLD_BYTES: i64 = 5000;

    fn server_named(name: &str) -> Server {
        Server {
            host: "foo".to_string(),
            port: 1234,
            name: name.to_string(),
        }
    }

    fn make_cluster_config() -> ClusterConfig {
        ClusterConfig {
            voters: vec![server_named("A"), server_named("B"), server_named("C")],
            voters_next: vec![server_named("A"), server_named("C"), server_named("D")],
        }
    }

    fn make_store() -> Store {
        let state_machine = FakeStateMachine::new();
        let snapshot = LogSnapshot {
            last: EntryId { term: 0, index: 0 },
            data: state_machine.create_snapshot(),
            config: make_cluster_config(),
        };
        Store::new(
            Arc::new(Mutex::new(state_machine)),
            snapshot,
            vec![],
            None, /* diagnostics */
            COMPACTION_THRESHOLD_BYTES,
            "testing-store",
        )
        .unwrap()
    }

    #[test]
    fn test_initial() {
        let store = make_store();
        assert_eq!(store.committed_index(), 0);
        assert_eq!(store.applied, 0);
        assert_eq!(store.next_index(), 1);
    }

    #[test]
    fn test_initial_with_entries() {
        let state_machine = FakeStateMachine::new();
        let snapshot = LogSnapshot {
            last: EntryId { term: 1, index: 4 },
            data: state_machine.create_snapshot(),
            config: make_cluster_config(),
        };

        let entries = vec![
            payload_entry(1, 5),
            payload_entry(1, 6),
            payload_entry(1, 7),
        ];
        let store = Store::new(
            Arc::new(Mutex::new(state_machine)),
            snapshot,
            entries,
            None,
            COMPACTION_THRESHOLD_BYTES,
            "test-store",
        )
        .unwrap();

        assert_eq!(store.committed_index(), 4);
        assert_eq!(store.applied, 4);
        assert_eq!(store.next_index(), 8);
    }

    #[test]
    fn test_conflict() {
        let mut store = make_store();
        store.append(71, Payload(Vec::new()));
        store.append(72, Payload(Vec::new()));
        store.append(73, Payload(Vec::new()));

        // No conflict, not present.
        assert!(!store.conflict(&EntryId { term: 1, index: 17 }));

        // No conflict, present.
        assert!(!store.conflict(&EntryId { term: 71, index: 1 }));

        // Conflict, present with different term.
        assert!(store.conflict(&EntryId { term: 70, index: 1 }));
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
        let receiver = store.add_listener(3);

        store.append(67, Payload(Vec::new()));
        store.append(67, Payload(Vec::new()));
        store.append(68, Payload(Vec::new()));

        store.commit_to(3).await;
        let output = receiver.now_or_never();
        assert!(output.is_some());

        let result = output.unwrap().unwrap();
        assert_eq!(3, result.index);
        assert_eq!(68, result.term);
    }

    #[tokio::test]
    async fn test_listener_multi() {
        let mut store = make_store();
        let receiver1 = store.add_listener(1);
        let receiver2 = store.add_listener(2);
        let receiver3 = store.add_listener(1);

        store.append(67, Payload(Vec::new()));
        store.commit_to(1).await;

        assert!(receiver1.now_or_never().is_some());
        assert!(receiver2.now_or_never().is_none());
        assert!(receiver3.now_or_never().is_some());
    }

    #[tokio::test]
    async fn test_listener_past() {
        let mut store = make_store();

        store.append(67, Payload(Vec::new()));
        store.append(67, Payload(Vec::new()));
        store.commit_to(2).await;

        let receiver = store.add_listener(1);
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
        assert_eq!(store.get_latest_snapshot().last.term, 0);
        assert_eq!(store.get_latest_snapshot().last.index, 0);

        let cluster_config = make_cluster_config();
        let snap = LogSnapshot {
            last: EntryId {
                term: 17,
                index: 22,
            },
            data: Bytes::from(""),
            config: cluster_config.clone(),
        };
        store.install_snapshot(snap.clone()).await;
        assert_eq!(22, store.committed_index());
        assert_eq!(store.config_info.latest_applied, cluster_config);

        let new_snap = store.get_latest_snapshot();
        assert_eq!(new_snap.last, snap.last);
        assert_eq!(new_snap.data, snap.data);
        assert_eq!(new_snap.config, snap.config);
    }

    #[test]
    fn test_config_info_append() {
        let mut store = make_store();
        assert!(store.config_info.latest_appended.is_none());

        store.append(17, Payload(Vec::new()));
        assert!(store.config_info.latest_appended.is_none());

        let config = ClusterConfig {
            voters: vec![],
            voters_next: vec![],
        };
        store.append(17, Config(config.clone()));
        assert!(!store.config_info.latest_appended.is_none());
    }

    #[tokio::test]
    async fn test_config_info_append_all() {
        let mut store = make_store();
        assert!(store.get_config_info().latest_appended.is_none());

        store
            .append_all(&vec![
                payload_entry(12, 1),
                payload_entry(12, 2),
                payload_entry(12, 3),
            ])
            .await;
        assert!(store.get_config_info().latest_appended.is_none());

        let voters = 7;
        store
            .append_all(&vec![
                payload_entry(12, 4),
                config_entry(12, 5, voters),
                payload_entry(12, 6),
            ])
            .await;
        assert!(store.get_config_info().latest_appended.is_some());
        match store
            .get_config_info()
            .latest_appended
            .unwrap()
            .data
            .unwrap()
        {
            Config(config) => assert_eq!(voters, config.voters.len()),
            _ => panic!("bad entry contents"),
        }

        let voters = 5;
        store
            .append_all(&vec![
                config_entry(12, 7, 3),
                config_entry(12, 8, 3),
                config_entry(12, 9, voters),
            ])
            .await;
        assert!(store.get_config_info().latest_appended.is_some());
        match store
            .get_config_info()
            .latest_appended
            .unwrap()
            .data
            .unwrap()
        {
            Config(config) => assert_eq!(voters, config.voters.len()),
            _ => panic!("bad entry contents"),
        }
        assert!(!store.get_config_info().committed);

        store.commit_to(9).await;
        assert!(store.get_config_info().committed);
    }

    #[tokio::test]
    async fn test_config_info_apply() {
        let mut store = make_store();
        let initial_config = store.get_config_info().latest_applied;

        // Append and commit a payload, should not change applied config.
        let entry1 = store.append(1, Payload(Vec::new()));
        store.commit_to(entry1.index).await;
        let config_after_payload = store.get_config_info().latest_applied;
        assert_eq!(initial_config, config_after_payload);

        // Append and commit a new config, should change applied config.
        let mut new_config = initial_config.clone();
        new_config.voters = vec![server_named("X")];
        let entry2 = store.append(1, Config(new_config.clone()));
        store.commit_to(entry2.index).await;
        let config_after_config = store.get_config_info().latest_applied;
        assert_eq!(new_config, config_after_config);
    }

    #[test]
    fn test_latest_no_appended() {
        let applied_config = make_config(3);
        let info = ConfigInfo {
            latest_appended: None,
            committed: false,
            latest_applied: applied_config.clone(),
        };

        let (latest, committed) = info.latest();
        assert_eq!(latest, applied_config);
        assert!(committed);
    }

    #[test]
    fn test_latest_appended_uncommitted() {
        let appended_config = make_config(5);
        let entry = Entry {
            id: Some(EntryId { term: 1, index: 1 }),
            data: Some(Config(appended_config.clone())),
        };
        let info = ConfigInfo::make_with_appended(entry, false);

        let (latest, committed) = info.latest();
        assert_eq!(latest, appended_config);
        assert!(!committed);
    }

    #[test]
    fn test_latest_appended_committed() {
        let appended_config = make_config(5);
        let entry = Entry {
            id: Some(EntryId { term: 1, index: 1 }),
            data: Some(Config(appended_config.clone())),
        };
        let info = ConfigInfo::make_with_appended(entry, true);

        let (latest, committed) = info.latest();
        assert_eq!(latest, appended_config);
        assert!(committed);
    }

    fn make_config(num_voters: usize) -> ClusterConfig {
        let mut voters = Vec::new();
        for i in 0..num_voters {
            voters.push(Server {
                host: "localhost".to_string(),
                port: i as i32,
                name: format!("server-{}", i),
            });
        }
        ClusterConfig {
            voters,
            voters_next: vec![],
        }
    }

    fn payload_entry(term: i64, index: i64) -> Entry {
        Entry {
            id: Some(EntryId { term, index }),
            data: Some(Payload(Vec::new())),
        }
    }

    fn config_entry(term: i64, index: i64, num_voters: usize) -> Entry {
        Entry {
            id: Some(EntryId { term, index }),
            data: Some(Config(make_config(num_voters))),
        }
    }
}
