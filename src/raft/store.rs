use async_std::sync::{Arc, Mutex};
use bytes::Bytes;
use futures::channel::oneshot::{Receiver, Sender, channel};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use tonic::{Code, Status};
use tracing::{debug, info, warn};

use crate::raft::log::LogSlice;
use crate::raft::persistence::{Persistence, PersistenceOptions};
use crate::raft::raft_common_proto::entry::Data;
use crate::raft::raft_common_proto::entry::Data::Config;
use crate::raft::raft_common_proto::{Entry, EntryId, Server};
use crate::raft::{StateMachine, persistence};

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

// TODO(dino): This module would benefit from a pass where we replace expect() with
// explicitly returned errors across the board. This is an invasive change.

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

        // Perform a one-time write to make sure that we start off in a state where we've
        // created the persisted files. Otherwise, partial writes might create a persisted
        // state that cannot be read (e.g., because only some files exist).
        result.persist_all().await;

        // TODO(dino): Make sure we initialize the cluster constituents correctly.

        result
    }

    // Reads state from the persistence instance and installs it in this store.
    async fn restore_persisted(&mut self) {
        if let Some(mut loaded) = self.persistence.read().await.expect("read persisted state") {
            loaded
                .validate()
                .expect("state loaded from disk is invalid");
            loaded.trim_entries();

            let snap = loaded.snapshot;
            let last = snap.last.clone();

            self.voted_for = loaded.voted_for;
            self.term = loaded.term;
            self.log = LogSlice::new(last);

            let status = self.install(snap.snapshot, snap.last, loaded.entries).await;
            assert_eq!(
                &status.code(),
                &Code::Ok,
                "Failed install, status: {}",
                &status
            );
        }
    }

    // Writes all the current persistent state.
    async fn persist_all(&self) {
        self.persistence
            .write(
                self.term,
                &self.voted_for,
                self.log.entries(),
                &self.snapshot.snapshot,
                &self.snapshot.last,
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
        // Check that the snapshot does indeed take us into the future.
        if last.index < self.applied {
            return Status::invalid_argument(format!(
                "[{}] Refused to install snapshot that would go back in time. Last applied index in snapshot is {}, our latest applied index is {}",
                &self.name, &last.index, &self.applied
            ));
        }

        // Store the new snapshot, including writing it to persistence.
        self.update_snapshot(LogSnapshot {
            last,
            snapshot: snapshot.clone(),
        })
        .await;

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
        match locked_machine.load_snapshot(&snapshot) {
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

        // Go through listeners and have them catch up to the current commit.
        self.resolve_listeners();

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
    use crate::raft::persistence::{FilePersistenceOptions, PersistenceOptions};
    use crate::raft::raft_common_proto::entry::Data::Payload;
    use crate::raft::raft_common_proto::{ClusterConfig, Server};
    use crate::raft::testing::FakeStateMachine;
    use futures::FutureExt;
    use tempfile::TempDir;

    const COMPACTION_THRESHOLD_BYTES: i64 = 5000;

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
    async fn test_install_snapshot_resolves_listeners() {
        let fixture = Fixture::new().await;
        let mut store = fixture.make_store().await;
        assert_eq!(store.get_latest_snapshot().last.term, -1);
        assert_eq!(store.get_latest_snapshot().last.index, -1);

        // Add a listener for a commit that will never go through here.
        let listener = store.add_listener(14);

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

        // The listener should be cancelled because the commit we created before the
        // snapshot was installed is not the one that went through.
        let result = listener.await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_config_info_append() {
        let fixture = Fixture::new().await;
        let mut store = fixture.make_store().await;
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
        let fixture = Fixture::new().await;
        let mut store = fixture.make_store().await;
        assert!(store.get_config_info().latest.is_none());

        store
            .append_all(&vec![
                payload_entry(12, 0),
                payload_entry(12, 1),
                payload_entry(12, 2),
            ])
            .await;
        assert!(store.get_config_info().latest.is_none());

        let voters = 7;
        store
            .append_all(&vec![
                payload_entry(12, 3),
                config_entry(12, 4, voters),
                payload_entry(12, 5),
            ])
            .await;
        assert!(store.get_config_info().latest.is_some());
        match store.get_config_info().latest.unwrap().data.unwrap() {
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
    async fn test_restore_persisted_snapshot() {
        let fixture = Fixture::new().await;

        let snapshot_bytes = Bytes::from("some snapshot");
        let entry_id = EntryId {
            term: 15,
            index: 19,
        };

        // Make some changes with a first store, then let it go out of scope.
        {
            let mut store = fixture.make_store().await;
            let status = store
                .install_snapshot(snapshot_bytes.clone(), entry_id.clone())
                .await;
            assert_eq!(status.code(), Code::Ok);
        }

        // Now create another store backed by the same directory and check we can load contents.
        {
            let store = fixture.make_store().await;
            assert_eq!(&store.snapshot.last, &entry_id);
            assert_eq!(&store.snapshot.snapshot, &snapshot_bytes);
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
                )
                .await
                .expect("failed to write");
        }

        {
            let mut store = fixture.make_store().await;
            store.restore_persisted().await;
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
                Arc::new(Mutex::new(FakeStateMachine::new())),
                COMPACTION_THRESHOLD_BYTES,
                "testing-store",
            )
            .await
        }
    }

    fn persistence_options(temp_dir: &TempDir) -> PersistenceOptions {
        let dir_str = temp_dir.path().to_str().unwrap().to_string();
        PersistenceOptions::FilePersistence(FilePersistenceOptions {
            directory: dir_str,
            wipe: false,
        })
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
