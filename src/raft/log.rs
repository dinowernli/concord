use crate::raft::raft_proto;

use raft_proto::{Entry, EntryId};

// Represents a contiguous slice of a raft log.
pub struct LogSlice {
    entries: Vec<Entry>,

    // The sum of the sizes of all payloads in the stored entries.
    size_bytes: i64,

    // The id of the entry immediately *before* this log slice, or a (-1, -1)
    // sentinel entry if this slice starts at the beginning of time.
    previous_id: EntryId,
}

// The possible outcomes of asking a log slice whether an entry id is
// contained inside the slice.
#[derive(Debug, Clone, PartialEq)]
pub enum ContainsResult {
    // Indicates that the beginning of the slice has advanced past an entry.
    COMPACTED,

    // Indicates that an entry is present in the log slice.
    PRESENT,

    // Indicates that an entry with this index has yet to be added.
    ABSENT,
}

impl LogSlice {
    // Returns a new instance with no entries, starting at some position in
    // the middle of the log. The "previous_id" parameter holds the id of
    // the last element in the log *not present* in this slice, i.e., the entry
    // immediately before this slice starts.
    pub fn new(previous_id: EntryId) -> Self {
        LogSlice {
            entries: Vec::new(),
            size_bytes: 0,
            previous_id: previous_id,
        }
    }

    // Returns a new instance with no entries, representing the *beginning* of
    // the log, i.e., the next expected entry has index 0.
    pub fn initial() -> Self {
        let mut sentinel = EntryId::new();
        sentinel.set_term(-1);
        sentinel.set_index(-1);
        Self::new(sentinel)
    }

    // Adds a new entry to the end of the slice. Returns the id of the
    // newly appended entry.
    pub fn append(&mut self, term: i64, payload: Vec<u8>) -> EntryId {
        assert!(term >= self.last_known_id().get_term());

        let size_bytes = payload.len() as i64;
        let entry = create_entry(term, self.next_index(), payload);
        let entry_id = entry.get_id().clone();

        self.entries.push(entry);
        self.size_bytes += size_bytes;
        entry_id
    }

    // Returns the latest entry id currently in the log slice.
    pub fn last_id(&self) -> Option<EntryId> {
        self.entries.last().map(|entry| entry.get_id().clone())
    }

    // Returns the expected index of the next element added to the log.
    pub fn next_index(&self) -> i64 {
        self.last_known_id().get_index() + 1
    }

    // Returns true if the supplied last entry id is at least as up-to-date
    // as the slice of the log tracked by this instance.
    pub fn is_up_to_date(&self, other_last: &EntryId) -> bool {
        let this_last = match self.entries.last() {
            Some(entry) => entry.get_id(),
            None => &self.previous_id,
        };

        if this_last.get_term() != other_last.get_term() {
            return other_last.get_term() > this_last.get_term();
        }

        // Terms are equal, last index decides.
        return other_last.get_index() >= this_last.get_index();
    }

    // Returns true if the supplied entry is present in this slice.
    pub fn contains(&self, query: &EntryId) -> ContainsResult {
        if query.get_index() <= self.previous_id.get_index() {
            assert!(query.get_term() <= self.previous_id.get_term());
            return ContainsResult::COMPACTED;
        }

        let idx = self.local_index(query.get_index());
        if idx >= self.entries.len() {
            return ContainsResult::ABSENT;
        }

        let &entry_id = &self.entries[idx as usize].get_id();
        assert!(entry_id == query);
        return ContainsResult::PRESENT;
    }

    // Adds the supplied entries to the end of the slice. Any conflicting
    // entries are replaced. Any existing entries with indexes higher than the
    // supplied entries are pruned.
    pub fn append_all(&mut self, entries: &[Entry]) {
        assert!(!entries.is_empty(), "append_all called with empty entries");

        for entry in entries {
            let size_bytes = entry.payload.len() as i64;
            let index = entry.get_id().get_index();
            if index == self.next_index() {
                self.entries.push(entry.clone());
                self.size_bytes += size_bytes;
            } else {
                let local_index = self.local_index(index);
                self.size_bytes -= self.entries[local_index].payload.len() as i64;
                self.entries[local_index] = entry.clone();
                self.size_bytes += size_bytes;
            }
        }

        // Remove anything that comes after the end of the provided entries.
        let last = entries.last().unwrap().get_id();
        let local_index = self.local_index(last.get_index());
        for entry in self.entries.drain((local_index + 1)..) {
            self.size_bytes -= entry.payload.len() as i64;
        }
    }

    // Returns the total size in bytes of all stored payloads. This is an
    // approximation of the total memory occupied by this instance. Note that
    // the size of the entry metadata and other meta structures are not
    // included in this returned value).
    pub fn size_bytes(&self) -> i64 {
        self.size_bytes as i64
    }

    // Returns all entries strictly after the supplied id. Must only be called
    // if the supplied entry id is present in the slice.
    pub fn get_entries_after(&self, entry_id: &EntryId) -> Vec<Entry> {
        // We can only actually return the entries we have, i.e., starting with
        // the entry immediately following "self.previous_id".
        assert!(entry_id.index >= self.previous_id.index);

        let local_start_idx = self.local_index(entry_id.get_index() + 1);
        if local_start_idx >= self.entries.len() {
            return Vec::new();
        }

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

    // Returns the entry at the supplied index. Must only be called if the index
    // is present in the slice.
    pub fn entry_at(&self, index: i64) -> Entry {
        self.entries.get(self.local_index(index)).unwrap().clone()
    }

    // Returns the position in the slice vector associated with the supplied
    // log index. Must only be called if the index is known to be within range
    // of this slice.
    fn local_index(&self, index: i64) -> usize {
        let adjusted = index - self.previous_id.get_index() - 1;
        assert!(adjusted >= 0, "adjusted index out of range");
        adjusted as usize
    }

    // Returns the highest index known to exist (even if the entry is not held
    // in this instance). Returns -1 if there are no known entries at all.
    fn last_known_id(&self) -> &EntryId {
        match self.entries.last() {
            Some(entry) => entry.get_id(),
            None => &self.previous_id,
        }
    }
}

fn create_entry(term: i64, index: i64, payload: Vec<u8>) -> Entry {
    let mut entry_id = EntryId::new();
    entry_id.set_term(term);
    entry_id.set_index(index);

    let mut entry = Entry::new();
    entry.set_id(entry_id);
    entry.set_payload(payload);
    entry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let l = LogSlice::initial();

        assert_eq!(l.contains(&entry_id(-1, -1)), ContainsResult::COMPACTED);

        let after = l.get_entries_after(&entry_id(-1, -1));
        assert!(after.is_empty());

        assert_eq!(0, l.next_index());
        assert!(l.last_id().is_none());
    }

    #[test]
    fn test_size_bytes() {
        let mut l = LogSlice::initial();
        assert_eq!(0, l.size_bytes());

        l.append(13 /* term */, payload_of_size(25));
        assert_eq!(25, l.size_bytes());

        l.append(13 /* term */, payload_of_size(4));
        assert_eq!(29, l.size_bytes());
    }

    #[test]
    fn test_single_entry() {
        let mut l = LogSlice::initial();
        l.append(72 /* term */, "some payload".as_bytes().to_vec());

        let expected_id = entry_id(72 /* term */, 0 /* index */);

        assert_eq!(&l.last_id().unwrap(), &expected_id);
        assert_eq!(&l.id_at(0), &expected_id);
        assert_eq!(1, l.next_index());
    }

    #[test]
    fn test_last() {
        let l = create_default_slice();
        assert_eq!(74, l.last_id().unwrap().get_term());
    }

    #[test]
    fn test_contains() {
        let l = create_default_slice();

        // Special sentinel case.
        assert_eq!(ContainsResult::COMPACTED, l.contains(&entry_id(-1, -1)));

        // Check a few existing entries.
        assert_eq!(ContainsResult::PRESENT, l.contains(&entry_id(73, 2)));
        assert_eq!(ContainsResult::PRESENT, l.contains(&entry_id(73, 3)));

        // First index not present.
        let next = l.next_index();
        assert_eq!(6, next);
        assert_eq!(ContainsResult::ABSENT, l.contains(&entry_id(95, next)));
        assert_eq!(ContainsResult::ABSENT, l.contains(&entry_id(12, next)));
    }

    #[test]
    fn test_id_at_valid() {
        let l = create_default_slice();

        let i = l.id_at(0);
        assert_eq!(0, i.get_index());
        assert_eq!(71, i.get_term());

        let j = l.id_at(4);
        assert_eq!(4, j.get_index());
        assert_eq!(73, j.get_term());
    }

    #[test]
    #[should_panic]
    fn test_id_at_invalid() {
        let l = create_default_slice();
        l.id_at(l.next_index());
    }

    #[test]
    fn test_is_up_to_date() {
        let l = create_default_slice();

        // Not up to date, l contains a newer entry.
        assert!(!l.is_up_to_date(&entry_id(73, 5)));

        // Not up to date, l contains a newer term.
        assert!(!l.is_up_to_date(&entry_id(69, 12)));

        // Should be up to date.
        assert!(l.is_up_to_date(&entry_id(74, 5)));
        assert!(l.is_up_to_date(&entry_id(75, 5)));
        assert!(l.is_up_to_date(&entry_id(75, 17)));
    }

    #[test]
    #[should_panic]
    fn test_append_bad_term() {
        let mut l = create_default_slice();
        l.append(73, "bad term".as_bytes().to_vec());
    }

    #[test]
    fn test_append() {
        let mut l = create_default_slice();
        let id = l.append(74, "bad term".as_bytes().to_vec());
        assert_eq!(74, id.get_term());
        assert_eq!(6, id.get_index());
    }

    #[test]
    fn test_append_all() {
        let mut l = create_default_slice();

        // Validate the initial state.
        assert_eq!(l.id_at(0).get_index(), 0);
        assert_eq!(l.last_id().unwrap().index, 5);
        let initial_size_bytes = 6;
        assert_eq!(l.size_bytes(), initial_size_bytes);

        // This should replace entries 3 and 4, and remove entry 5
        l.append_all(&[
            entry(75, 3 /* index */, 10 /* size */),
            entry(75, 4 /* index */, 10 /* size */),
        ]);
        assert_eq!(l.id_at(0).get_index(), 0);
        assert_eq!(l.last_id().unwrap().index, 4);
        let size_bytes = initial_size_bytes - 3 + 20;
        assert_eq!(l.size_bytes(), size_bytes);

        // This should just append a new entry 5
        l.append_all(&[entry(76, 5 /* index */, 10 /* size */)]);
        assert_eq!(l.last_id().unwrap().index, 5);
        let new_size_bytes = size_bytes + 10;
        assert_eq!(l.size_bytes(), new_size_bytes);
    }

    #[test]
    #[should_panic]
    fn test_append_all_empty() {
        let mut l = create_default_slice();
        l.append_all(&[]);
    }

    #[test]
    #[should_panic]
    fn test_append_far_future_index() {
        let mut l = LogSlice::new(entry_id(75, 10));

        // Make an append with indexes that are far in the future.
        l.append_all(&[
            entry(75, 45 /* index */, 10 /* size */),
            entry(75, 46 /* index */, 10 /* size */),
        ]);
    }

    fn create_default_slice() -> LogSlice {
        let mut result = LogSlice::initial();
        result.append(71 /* term */, payload_of_size(1));
        result.append(72 /* term */, payload_of_size(1));
        result.append(73 /* term */, payload_of_size(1));
        result.append(73 /* term */, payload_of_size(1));
        result.append(73 /* term */, payload_of_size(1));
        result.append(74 /* term */, payload_of_size(1));
        result
    }

    fn payload_of_size(size_bytes: i64) -> Vec<u8> {
        [3].repeat(size_bytes as usize)
    }

    fn entry_id(term: i64, index: i64) -> EntryId {
        let mut result = EntryId::new();
        result.set_index(index);
        result.set_term(term);
        return result;
    }

    fn entry(term: i64, index: i64, payload_size_bytes: i64) -> Entry {
        let mut result = Entry::new();
        result.set_payload(payload_of_size(payload_size_bytes));
        result.set_id(entry_id(term, index));
        return result;
    }
}
