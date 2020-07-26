use crate::raft::raft_proto;

use raft_proto::{Entry, EntryId};

// Represents a contiguous slice of a raft log.
pub struct LogSlice {
    entries: Vec<Entry>,
}

// The possible outcomes of asking a log slice whether an entry id is
// contained inside the slice.
#[derive(Debug, Clone, PartialEq)]
pub enum ContainsResult {
    // Indicates that the beginning of the slice has advanced past an entry.
    COMPACTED,

    // Indicates that an entry is present in the log slice.
    PRESENT,

    // Indicates that an entry index is present, but with a different term.
    MISMATCH,

    // Indicates that an entry with this index has yet to be added.
    ABSENT,
}

impl LogSlice {
    pub fn new() -> Self {
        LogSlice {
            entries: Vec::new(),
        }
    }

    // Adds a new entry to the end of the slice. Returns the id of the
    // newly appended entry.
    pub fn append(&mut self, term: i64, payload: Vec<u8>) -> EntryId {
        let mut entry_id = EntryId::new();
        entry_id.set_term(term);
        entry_id.set_index(self.next_index());

        let mut entry = Entry::new();
        entry.set_id(entry_id.clone());
        entry.set_payload(payload);

        self.entries.push(entry);
        entry_id
    }

    // Returns the lowest index currently in the log slice.
    pub fn first_index(&self) -> Option<i64> {
        self.entries.first().map(|entry| entry.get_id().index)
    }

    // Returns the latest entry id currently in the log slice.
    pub fn last_id(&self) -> Option<EntryId> {
        self.entries.last().map(|entry| entry.get_id().clone())
    }

    // Returns the highest index currently in the log slice.
    pub fn last_index(&self) -> Option<i64> {
        self.last_id().map(|id| id.index)
    }

    // Returns the expected index of the next element which will be added to
    // the log. If the log is empty, this is index 0, otherwise it's the index
    // one greater than the last entry present.
    pub fn next_index(&self) -> i64 {
        match self.entries.last() {
            Some(entry) => entry.get_id().get_index() + 1,
            None => 0,
        }
    }

    // Returns true if the supplied last entry id is at least as up-to-date
    // as the slice of the log tracked by this instance.
    pub fn is_up_to_date(&self, last: &EntryId) -> bool {
        if self.entries.is_empty() {
            return true;
        }

        let this_last = self.last_id().unwrap();
        if this_last.get_term() != last.get_term() {
            return last.get_term() > this_last.get_term();
        }

        // Terms are equal, last index decides.
        return last.get_index() >= this_last.get_index();
    }

    // Returns true if the supplied entry
    pub fn contains(&self, query: &EntryId) -> ContainsResult {
        // Special case where we've never seen an entry and the request comes
        // in for the special "-1" index.
        if query.index == -1 {
            return if self.starts_at_beginning_of_time() {
                ContainsResult::PRESENT
            } else {
                ContainsResult::COMPACTED
            };
        }

        let first = self.first_index();
        if first.is_none() {
            // Assumes that this can only happen when the log slice represents
            // the beginning of time and has never seen an entry.
            return ContainsResult::ABSENT;
        }

        if first.unwrap() > query.index {
            return ContainsResult::COMPACTED;
        }

        // Translate the requested index into the vector index.
        let idx = self.local_index(query.index);
        if idx >= self.entries.len() {
            return ContainsResult::ABSENT;
        }

        let &entry_id = &self.entries[idx as usize].get_id();
        assert_eq!(entry_id.get_index(), query.get_index());
        if entry_id.get_term() == query.get_term() {
            ContainsResult::PRESENT
        } else {
            ContainsResult::MISMATCH
        }
    }

    // Adds the supplied entries to the end of the slice. Any conflicting
    // entries are replaced. Any existing entries with indexes higher than the
    // supplied entries are pruned.
    pub fn append_all(&mut self, entries: &[Entry]) {
        assert!(!entries.is_empty(), "append_all called with empty entries");

        for entry in entries {
            let index = entry.get_id().get_index();
            if index == self.next_index() {
                self.entries.push(entry.clone());
            } else {
                let local_index = self.local_index(index);
                self.entries[local_index] = entry.clone();
            }
        }

        let last = entries.last().unwrap().get_id();
        let local_index = self.local_index(last.get_index());
        self.entries.truncate(local_index + 1);
    }

    // Returns all entries strictly after the supplied id. Must only be called
    // if the supplied entry id is present in the slice.
    pub fn get_entries_after(&self, entry_id: &EntryId) -> Vec<Entry> {
        let start_idx = entry_id.get_index() + 1;
        if start_idx == 0 && self.entries.is_empty() {
            return Vec::new();
        }

        let local_start_idx = self.local_index(start_idx);

        let mut result = Vec::new();
        for value in &self.entries[local_start_idx..] {
            result.push(value.clone());
        }
        result
    }

    // Returns the entry id for the entry at the supplied index. Must only be
    // called if the index is present in the slice.
    pub fn id_at(&self, index: i64) -> EntryId {
        self.entries
            .get(self.local_index(index))
            .unwrap()
            .get_id()
            .clone()
    }

    // Returns the position in the slice vector associated with the supplied
    // log index. Must only be called if the index is known to be within range
    // of this slice.
    fn local_index(&self, index: i64) -> usize {
        let adjusted = index - self.first_index().expect("cannot be empty");
        assert!(adjusted >= 0, "adjusted index out of range");
        adjusted as usize
    }

    // Returns true if this slice starts at the beginning of time.
    fn starts_at_beginning_of_time(&self) -> bool {
        self.entries.is_empty() || self.first_index().unwrap() == 0
    }
}
