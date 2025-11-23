use crate::raft::diagnostics::ServerDiagnostics;
use crate::raft::error::RaftError::Initialization;
use crate::raft::error::RaftResult;
use crate::raft::log::LogSlice;
use crate::raft::persistence::{Persistence, PersistenceOptions, PersistentState};
use crate::raft::raft_common_proto::entry::Data;
use crate::raft::raft_common_proto::entry::Data::Config;
use crate::raft::raft_common_proto::{ClusterConfig, Entry, EntryId, Server};
use crate::raft::{StateMachine, persistence};
use async_std::sync::{Arc, Mutex};
use bytes::Bytes;
use futures::channel::oneshot::{Receiver, Sender, channel};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
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
    diagnostics: Option<Arc<Mutex<ServerDiagnostics>>>,
    persistence: Box<dyn Persistence + Send>,

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
    latest_appended: Option<Entry>,

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
    pub async fn new(
        persistence_options: PersistenceOptions,
        initial_cluster_config: ClusterConfig,
        state_machine: Arc<Mutex<dyn StateMachine + Send>>,
        diagnostics: Option<Arc<Mutex<ServerDiagnostics>>>,
        compaction_threshold_bytes: i64,
        name: &str,
    ) -> RaftResult<Self> {
        // Initialize persistence and try to load state from previous instance.
        let persistence = persistence::new(persistence_options)
            .await
            .map_err(|e| Initialization(format!("Failed to create persistence: {:?}", e)))?;

        let loaded_state = persistence
            .read()
            .await
            .map_err(|e| Initialization(format!("Failed to read from persistence: {:?}", e)))?;

        // If none was loaded, create a new initial state to use below.
        let (persistent_state, slice) = match loaded_state {
            Some(state) => {
                let last = state.snapshot.last.clone();
                // TODO(dino): The slice just reflects the snapshot, not the entries. We currently
                // install the entries by calling "restore_persisted()" after creation. We should
                // figure out a way to initialize the full store correctly instead.
                let slice = LogSlice::new(last);
                (state, slice)
            }
            None => {
                let data = state_machine.lock().await.create_snapshot();
                let snapshot = LogSnapshot::make_initial(data, initial_cluster_config);
                let state = PersistentState {
                    term: 0,
                    voted_for: None,
                    snapshot,
                    entries: vec![],
                };
                (state, LogSlice::initial())
            }
        };

        let last = persistent_state.snapshot.last.index;
        let latest_applied_config = persistent_state.snapshot.config.clone();
        let config_info = ConfigInfo {
            latest_appended: None,
            committed: false,
            latest_applied: latest_applied_config,
        };

        let mut result = Self {
            name: name.to_string(),
            compaction_threshold_bytes,
            state_machine,
            diagnostics,
            persistence,

            // Persistent state.
            log: slice,
            snapshot: persistent_state.snapshot,
            term: persistent_state.term,
            voted_for: persistent_state.voted_for,

            // Non-persistent. Just bookkeeping.
            committed: last,
            applied: last,
            config_info,

            listener_uid: 0,
            listeners: BTreeSet::new(),
        };

        // Try restoring state from our persistence source. It's important that we do this
        // immediately before calling anything else on the result store, to avoid the store
        // clobbering the persisted state as part of other operations.
        result
            .restore_persisted()
            .await
            .map_err(|e| Initialization(format!("Failed to restore persisted: {:?}", e)))?;

        // Perform a one-time write to make sure that we start off in a state where we've
        // created the persisted files. Otherwise, partial writes might create a persisted
        // state that cannot be read (e.g., because only some files exist).
        result.persist_all().await;

        Ok(result)
    }

    // Reads state from the persistence instance and installs it in this store.
    async fn restore_persisted(&mut self) -> Result<(), String> {
        let read = self
            .persistence
            .read()
            .await
            .map_err(|e| format!("Failed to read from persistence: {:?}", e))?;
        if let Some(mut loaded) = read {
            loaded
                .validate()
                .map_err(|e| format!("Failed to validate persisted contents: {:?}", e))?;
            loaded.trim_entries();

            let snap = loaded.snapshot;
            let last = snap.last.clone();

            self.voted_for = loaded.voted_for;
            self.term = loaded.term;
            self.log = LogSlice::new(last);

            let status = self.install(snap, loaded.entries).await;
            if &status.code() != &Code::Ok {
                return Err(format!(
                    "Failed to install loaded snapshot: {}",
                    status.to_string()
                ));
            }
        }
        Ok(())
    }

    // Writes all the current persistent state.
    async fn persist_all(&self) {
        self.persistence
            .write(
                self.term,
                &self.voted_for,
                self.log.entries(),
                &self.snapshot.data,
                &self.snapshot.last,
                &self.snapshot.config,
            )
            .await
            .expect("write persisted state");
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

        let clean_append = self.log.append_all(entries);

        if is_any_config {
            self.update_config_info();
        }

        // TODO(dino): If uncommented, this logic appears to occasionally produce non-consecutive
        // log slices on disk. However, if we always flush the entire contents, this doesn't happen.
        // Needs debugging. Repro with `cargo run -- -r -w` and wait for crash.
        //
        // if clean_append {
        //     // Optimization for the case where we just appended entries to the back of the slice
        //     // and didn't remove anything - persistence can just append the entries.
        //     self.persistence
        //         .append_entries(entries)
        //         .await
        //         .expect("append entries");
        // } else {
        //     // Otherwise, replace the persisted entries.
        //     self.persist_entries().await;
        // }

        self.persist_entries().await;
    }

    async fn persist_entries(&mut self) {
        // TODO(dino): Important to make sure we don't accept or serve any more RPCs if this fails
        self.persistence
            .write_entries(self.log.entries())
            .await
            .expect("write entries");
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
            .write_snapshot(&snapshot.data, &snapshot.last, &snapshot.config)
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
            let latest_applied_config = self.config_info.latest_applied.clone();
            let snap = LogSnapshot {
                data: self.state_machine.lock().await.create_snapshot(),
                last: latest_id.clone(),
                config: latest_applied_config,
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

        // Store the new snapshot, including writing it to persistence.
        self.update_snapshot(snapshot).await;

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

        // Make sure we've persisted the latest state, even if nothing appended.
        self.persist_entries().await;

        // Update the state machine.
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
    async fn report_apply(
        diagnostics: Option<Arc<Mutex<ServerDiagnostics>>>,
        entry_id: &EntryId,
        bytes: &Vec<u8>,
    ) {
        let diag = diagnostics.clone();
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
        let diagnostics = self.diagnostics.clone();
        while self.applied < self.committed {
            self.applied = self.applied + 1;
            let entry = self.log.entry_at(self.applied);
            let entry_id = entry.id.expect("id").clone();

            if let Some(data) = entry.data {
                match data {
                    Data::Payload(bytes) => {
                        // Report for debugging purposes.
                        Self::report_apply(diagnostics.clone(), &entry_id, &bytes).await;

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
#[derive(Debug, Clone)]
pub struct LogSnapshot {
    // The id of the latest entry included in the snapshot.
    pub last: EntryId,

    // A snapshot of the state machine.
    pub data: Bytes,

    // The cluster configuration present in the latest included entry
    // that contains a cluster configuration.
    pub config: ClusterConfig,
}

impl LogSnapshot {
    pub fn make_initial(initial_bytes: Bytes, config: ClusterConfig) -> Self {
        LogSnapshot {
            last: EntryId {
                term: -1,
                index: -1,
            },
            data: initial_bytes,
            config,
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
    use crate::raft::persistence::{FilePersistenceOptions, PersistenceOptions};
    use crate::raft::raft_common_proto::entry::Data::Payload;
    use crate::raft::raft_common_proto::{ClusterConfig, Server};
    use crate::raft::testing::FakeStateMachine;
    use futures::FutureExt;
    use tempfile::TempDir;

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

    async fn make_store() -> Store {
        let state_machine = FakeStateMachine::new();
        Store::new(
            PersistenceOptions::NoPersistenceForTesting,
            make_cluster_config(),
            Arc::new(Mutex::new(state_machine)),
            None, /* diagnostics */
            COMPACTION_THRESHOLD_BYTES,
            "testing-store",
        )
        .await
        .expect("make")
    }

    #[tokio::test]
    async fn test_initial() {
        let fixture = Fixture::new().await;
        let store = fixture.make_store().await;
        assert_eq!(store.committed_index(), -1);
        assert_eq!(store.applied, -1);
        assert_eq!(store.next_index(), 0);
    }

    #[tokio::test]
    #[should_panic]
    async fn test_commit_to_bad_index() {
        let fixture = Fixture::new().await;
        let mut store = fixture.make_store().await;
        store.append(2, Payload(Vec::new()));

        // Attempt to "commit to" a value which hasn't yet been appended.
        store.commit_to(17).await;
    }

    #[tokio::test]
    async fn test_commit_to() {
        let fixture = Fixture::new().await;
        let mut store = fixture.make_store().await;
        let eid = store.append(2, Payload(Vec::new()));

        // Should succeed.
        store.commit_to(eid.index).await;

        // Should succeed (noop).
        store.commit_to(eid.index - 1).await;
    }

    #[tokio::test]
    async fn test_listener() {
        let fixture = Fixture::new().await;
        let mut store = fixture.make_store().await;
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
        let fixture = Fixture::new().await;
        let mut store = fixture.make_store().await;
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
        let fixture = Fixture::new().await;
        let mut store = fixture.make_store().await;

        store.append(67, Payload(Vec::new()));
        store.append(67, Payload(Vec::new()));
        store.commit_to(1).await;

        let receiver = store.add_listener(0);
        assert!(receiver.now_or_never().is_some());
    }

    #[tokio::test]
    async fn test_compaction() {
        let fixture = Fixture::new().await;
        let mut store = fixture.make_store().await;
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
        let fixture = Fixture::new().await;
        let mut store = fixture.make_store().await;
        assert_eq!(store.get_latest_snapshot().last.term, -1);
        assert_eq!(store.get_latest_snapshot().last.index, -1);

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

    #[tokio::test]
    async fn test_install_snapshot_resolves_listeners() {
        let fixture = Fixture::new().await;
        let mut store = fixture.make_store().await;
        assert_eq!(store.get_latest_snapshot().last.term, -1);
        assert_eq!(store.get_latest_snapshot().last.index, -1);

        // Add a listener for a commit that will never go through here.
        let listener = store.add_listener(14);

        let log_snapshot = LogSnapshot {
            last: EntryId {
                term: 17,
                index: 22,
            },
            data: Bytes::from(""),
            config: make_cluster_config(),
        };
        store.install_snapshot(log_snapshot).await;
        assert_eq!(22, store.committed_index());

        // The listener should be cancelled because the commit we created before the
        // snapshot was installed is not the one that went through.
        let result = listener.await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_config_info_append() {
        let fixture = Fixture::new().await;
        let mut store = fixture.make_store().await;

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
        let fixture = Fixture::new().await;
        let mut store = fixture.make_store().await;
        assert!(store.get_config_info().latest_appended.is_none());

        store
            .append_all(&vec![
                payload_entry(12, 0),
                payload_entry(12, 1),
                payload_entry(12, 2),
            ])
            .await;
        assert!(store.get_config_info().latest_appended.is_none());

        let voters = 7;
        store
            .append_all(&vec![
                payload_entry(12, 3),
                config_entry(12, 4, voters),
                payload_entry(12, 5),
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
                config_entry(12, 6, 3),
                config_entry(12, 7, 3),
                config_entry(12, 8, voters),
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

        store.commit_to(8).await;
        assert!(store.get_config_info().committed);
    }

    #[tokio::test]
    async fn test_restore_persisted_snapshot() {
        let fixture = Fixture::new().await;

        let snapshot_bytes = Bytes::from("some snapshot");
        let entry_id = EntryId {
            term: 15,
            index: 19,
        };
        let log_snapshot = LogSnapshot {
            data: snapshot_bytes.clone(),
            last: entry_id,
            config: make_cluster_config(),
        };

        // Make some changes with a first store, then let it go out of scope.
        {
            let mut store = fixture.make_store().await;
            let status = store.install_snapshot(log_snapshot).await;
            assert_eq!(status.code(), Code::Ok);
        }

        // Now create another store backed by the same directory and check we can load contents.
        {
            let store = fixture.make_store().await;
            assert_eq!(&store.snapshot.last, &entry_id);
            assert_eq!(&store.snapshot.data, &snapshot_bytes);
        }
    }

    #[tokio::test]
    async fn test_restore_persisted_voted_for() {
        let fixture = Fixture::new().await;

        let voted_for = Some(server("some host", 1414));

        // Make some changes with a first store, then let it go out of scope.
        {
            let mut store = fixture.make_store().await;
            store.update_voted_for(&voted_for).await;
        }

        // Now create another store backed by the same directory and check we can load contents.
        {
            let store = fixture.make_store().await;
            assert_eq!(store.voted_for(), voted_for);
        }
    }

    #[tokio::test]
    async fn test_restore_persisted_term() {
        let fixture = Fixture::new().await;
        let term = 728;

        // Make some changes with a first store, then let it go out of scope.
        {
            let mut store = fixture.make_store().await;
            store.update_term_info(term, &None).await;
        }

        // Now create another store backed by the same directory and check we can load contents.
        {
            let store = fixture.make_store().await;
            assert_eq!(store.term(), term);
        }
    }

    #[tokio::test]
    async fn test_restore_persisted_entries() {
        let fixture = Fixture::new().await;

        // Make some changes with a first store, then let it go out of scope.
        {
            let mut store = fixture.make_store().await;
            store
                .append_all(&[
                    payload_entry(12, 0),
                    payload_entry(12, 1),
                    payload_entry(12, 2),
                ])
                .await;
            assert_eq!(2, store.last_known_log_entry_id().index);
        }

        // Now create another store backed by the same directory and check we can load contents.
        {
            let store = fixture.make_store().await;
            assert_eq!(2, store.last_known_log_entry_id().index);
        }
    }

    #[tokio::test]
    async fn test_restore_trims_entries_included_in_snapshot() {
        let fixture = Fixture::new().await;

        let entries = vec![
            payload_entry(4, 10),
            payload_entry(4, 11),
            payload_entry(4, 12),
            payload_entry(4, 13),
        ];
        let last_in_snapshot = EntryId { term: 4, index: 11 };
        let cluster_conf = make_cluster_config();

        {
            let store = fixture.make_store().await;
            store
                .persistence
                .write(
                    7,
                    &None,
                    &entries,
                    &Bytes::from("some snap"),
                    &last_in_snapshot,
                    &cluster_conf,
                )
                .await
                .expect("failed to write");
        }

        {
            let mut store = fixture.make_store().await;
            store.restore_persisted().await.expect("restore");
            let entries = store.log.entries().clone();

            // Only the last two entries should remain.
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].id.unwrap().index, 12);
            assert_eq!(entries[1].id.unwrap().index, 13);
        }
    }

    #[tokio::test]
    async fn test_todo() {
        // Major construction sites:
        // - Overhaul error handling to use less expect()
        // - Fix issue with the entry append optimization (see below)
        // - Include cluster configuration in snapshot so that snapshots work with reconfiguration
        // - Build a test harness [done] that allows crashing / stopping / disconnecting for better tests
        panic!(
            "Look investigate why the append optimization for writing entries to persistent storage produces non-consecutive files (see todo in code above)"
        );
    }

    #[tokio::test]
    async fn test_config_info_apply() {
        let mut store = make_store().await;
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

    struct Fixture {
        temp_dir: TempDir,
    }

    impl Fixture {
        async fn new() -> Fixture {
            Fixture {
                temp_dir: TempDir::new().unwrap(),
            }
        }

        async fn make_store(&self) -> Store {
            let options = persistence_options(&self.temp_dir);
            Store::new(
                options,
                make_cluster_config(),
                Arc::new(Mutex::new(FakeStateMachine::new())),
                None, /* diagnostics */
                COMPACTION_THRESHOLD_BYTES,
                "testing-store",
            )
            .await
            .expect("make")
        }
    }

    fn persistence_options(temp_dir: &TempDir) -> PersistenceOptions {
        let dir_str = temp_dir.path().to_str().unwrap().to_string();
        PersistenceOptions::FilePersistence(FilePersistenceOptions {
            directory: dir_str,
            wipe: false,
        })
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

    fn server(host: &str, port: i32) -> Server {
        Server {
            host: host.to_string(),
            port,
            name: String::new(),
        }
    }
}
