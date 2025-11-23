use crate::raft::raft_common_proto::entry::Data;
use crate::raft::raft_common_proto::entry::Data::{Config, Payload};
use crate::raft::raft_common_proto::{Entry, EntryId};

// Represents a contiguous slice of a raft log.
pub struct LogSlice {
    entries: Vec<Entry>,

    // The sum of the sizes of all payloads in the stored entries.
    size_bytes: i64,

    // The id of the entry immediately *before* this log slice, or a (-1, -1)
    // sentinel entry if this slice starts at the beginning of time.
    previous_id: EntryId,
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
            previous_id,
        }
    }

    // Returns a new instance with no entries, representing the *beginning* of
    // the log, i.e., the next expected entry has index 0.
    pub fn initial() -> Self {
        Self::new(EntryId {
            term: -1,
            index: -1,
        })
    }

    // Returns a reference to all entries present in this slice.
    pub fn entries(&self) -> &Vec<Entry> {
        &self.entries
    }

    // Adds a new entry to the end of the slice. Returns the id of the
    // newly appended entry.
    //
    // TODO(dino): this is super dangerous if not called from "store" because
    // store keeps track of the latest config entry. Once our call to
    // "latest_config_entry" becomes cheap, store won't have to cache it anymore
    // and this becomes a lot safer.
    pub fn append(&mut self, term: i64, data: Data) -> EntryId {
        assert!(term >= self.last_known_id().term);

        let entry = create_entry(term, self.next_index(), data);
        let bytes = size_bytes(&entry);
        let entry_id = entry.id.clone().expect("entry");

        self.entries.push(entry);
        self.size_bytes += bytes;
        entry_id
    }

    // Returns the highest index known to exist (even if the entry is not held
    // in this instance). Returns -1 if there are no known entries at all.
    pub fn last_known_id(&self) -> &EntryId {
        match self.entries.last() {
            Some(entry) => &entry.id.as_ref().expect("id"),
            None => &self.previous_id,
        }
    }

    // Returns the expected index of the next element added to the log.
    pub fn next_index(&self) -> i64 {
        self.last_known_id().index + 1
    }

    // Returns true if the supplied last entry id is at least as up-to-date
    // as the slice of the log tracked by this instance.
    pub fn is_up_to_date(&self, other_last: &EntryId) -> bool {
        let this_last = match self.entries.last() {
            Some(entry) => &entry.id.as_ref().expect("id"),
            None => &self.previous_id,
        };

        if this_last.term != other_last.term {
            return other_last.term > this_last.term;
        }

        // Terms are equal, last index decides.
        other_last.index >= this_last.index
    }

    // Returns true if the supplied entry id is present in this slice. Both the term and
    // the index need to match for the result to be true.
    fn contains(&self, query: &EntryId) -> bool {
        if query.index <= self.previous_id.index {
            return false;
        }

        let idx = self.local_index(query.index);
        if idx >= self.entries.len() {
            return false;
        }

        let &entry_id = &self.entries[idx].id.as_ref().expect("id");
        entry_id.index == query.index && entry_id.term == query.term
    }

    // Returns true if the supplied index lies before the range of entries present
    // in this log instance.
    pub fn is_index_compacted(&self, index: i64) -> bool {
        index <= self.previous_id.index
    }

    // Returns a copy of the latest appended entry containing a config.
    // Warning: this can be expensive because it walks the log backwards.
    // TODO(dino): cache this and make this cheap.
    pub fn latest_config_entry(&self) -> Option<Entry> {
        for i in (0..self.entries.len()).rev() {
            let entry = self.entries.get(i).unwrap();
            if matches!(&entry.data, Some(Config(_))) {
                return Some(entry.clone());
            }
        }
        None
    }

    // Adds the supplied entries to the end of the slice. Any conflicting
    // entries are replaced. Any existing entries with indexes higher than the
    // supplied entries are pruned.
    //
    // If this returns true, the operation was performed as a "clean append",
    // i.e., we appended every supplied entry to the end of the slice, and
    // removed no entries.
    //
    // TODO(dino): this is super dangerous if not called from "store" because
    // store keeps track of the latest config entry. Once our call to
    // "latest_config_entry" becomes cheap, store won't have to cache it anymore
    // and this becomes a lot safer.
    pub fn append_all(&mut self, entries: &[Entry]) -> bool {
        assert!(!entries.is_empty(), "append_all called with empty entries");

        let mut clean_append = true;

        for entry in entries {
            let len_bytes = size_bytes(&entry);
            let index = entry.id.as_ref().expect("id").index;

            if index == self.next_index() {
                self.entries.push(entry.clone());
                self.size_bytes += len_bytes;
            } else {
                let local_index = self.local_index(index);
                self.size_bytes -= size_bytes(&self.entries[local_index]);
                self.entries[local_index] = entry.clone();
                self.size_bytes += len_bytes;

                // If we had to replace an entry, this wasn't a clean append.
                clean_append = false;
            }
        }

        // Remove anything that comes after the end of the provided entries.
        let last = entries.last().unwrap().id.as_ref().expect("id");
        let local_index = self.local_index(last.index);
        for entry in self.entries.drain((local_index + 1)..) {
            self.size_bytes -= size_bytes(&entry);

            // If we had to remove trailing entries, this wasn't a clean append.
            clean_append = false;
        }

        clean_append
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

        let local_start_idx = self.local_index(entry_id.index + 1);
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
    // called if the index is known to this slice.
    //
    // Note that for the entry immediately before the start of this slice, the
    // entry id can be returned, but not the full entry.
    pub fn id_at(&self, index: i64) -> EntryId {
        if index == self.previous_id.index {
            return self.previous_id.clone();
        }
        let local_idx = self.local_index(index);
        assert!(local_idx <= self.entries.len());
        self.entries
            .get(local_idx)
            .unwrap()
            .id
            .as_ref()
            .expect("id")
            .clone()
    }

    // Returns the entry at the supplied index. Must only be called if the index
    // is present in the slice.
    pub fn entry_at(&self, index: i64) -> Entry {
        let local_idx = self.local_index(index);
        assert!(local_idx <= self.entries.len());
        self.entries.get(local_idx).unwrap().clone()
    }

    // Removes all entries up to and including the supplied id. Once this
    // returns, this instance starts immediately after the supplied id.
    pub fn prune_until(&mut self, entry_id: &EntryId) {
        assert!(
            entry_id.index >= self.previous_id.index,
            "Cannot prune into the past"
        );

        if self.contains(entry_id) {
            // We need to add 1 because the "drain()" call below is exclusive, but we want this
            // drain to be inclusive of the supplied entry.
            let local_idx = self.local_index(entry_id.index) + 1;
            for entry in self.entries.drain(0..local_idx) {
                self.size_bytes -= size_bytes(&entry);
            }
        } else {
            // Entry is not present, we just remove everything.
            self.entries.clear();
            self.size_bytes = 0;
        }
        self.previous_id = entry_id.clone();
    }

    // Returns the position in the slice vector associated with the supplied
    // log index. Must only be called if the index is known to be within range
    // of this slice.
    fn local_index(&self, index: i64) -> usize {
        let previous = self.previous_id.index;
        let adjusted = index - previous - 1;
        assert!(
            adjusted >= 0,
            "adjusted index out of range: adjusted={}, index={}, previous={}",
            adjusted,
            index,
            previous
        );
        adjusted as usize
    }
}

fn create_entry(term: i64, index: i64, data: Data) -> Entry {
    Entry {
        id: Some(EntryId { term, index }),
        data: Some(data),
    }
}

fn size_bytes(entry: &Entry) -> i64 {
    match &entry.data {
        Some(Payload(bytes)) => bytes.len() as i64,
        Some(Config(_)) => 0,
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::raft_common_proto::ClusterConfig;

    #[test]
    fn test_initial() {
        let l = LogSlice::initial();
        assert!(!l.contains(&entry_id(-1, -1)));

        let after = l.get_entries_after(&entry_id(-1, -1));
        assert!(after.is_empty());
        assert_eq!(0, l.next_index());
    }

    #[test]
    fn test_empty() {
        let previous = entry_id(65 /* term */, 17 /* index */);
        let l = LogSlice::new(previous.clone());

        assert_eq!(l.size_bytes(), 0);
        assert_eq!(l.next_index(), 18);
        assert!(l.latest_config_entry().is_none());
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
        l.append(
            72, /* term */
            Payload("some payload".as_bytes().to_vec()),
        );

        let expected_id = entry_id(72 /* term */, 0 /* index */);

        assert_eq!(&l.id_at(0), &expected_id);
        assert_eq!(1, l.next_index());
    }

    #[test]
    fn test_contains() {
        let l = create_default_slice();

        // Special sentinel case.
        assert!(!l.contains(&entry_id(-1, -1)));

        // Check a few existing entries.
        assert!(l.contains(&entry_id(73, 2)));
        assert!(l.contains(&entry_id(73, 3)));

        // Check the case where only index matches.
        assert!(!l.contains(&entry_id(999, 2)));
        assert!(!l.contains(&entry_id(999, 3)));

        // First index not present.
        let next = l.next_index();
        assert_eq!(6, next);
        assert!(!l.contains(&entry_id(95, next)));
        assert!(!l.contains(&entry_id(12, next)));
    }

    #[test]
    fn test_id_at_valid() {
        let l = create_default_slice();

        let i = l.id_at(0);
        assert_eq!(0, i.index);
        assert_eq!(71, i.term);

        let j = l.id_at(4);
        assert_eq!(4, j.index);
        assert_eq!(73, j.term);
    }

    #[test]
    #[should_panic]
    fn test_entry_at_previous() {
        let l = LogSlice::new(entry_id(75 /* term */, 17 /* index */));

        // Even though the id is known, the entry is not present.
        l.entry_at(17);
    }

    #[test]
    fn test_id_at_previous() {
        let previous = entry_id(75 /* term */, 17 /* index */);
        let l = LogSlice::new(previous.clone());
        assert_eq!(l.id_at(17), previous);
    }

    #[test]
    fn test_id_at_initial() {
        let l = LogSlice::initial();
        assert_eq!(l.id_at(-1), entry_id(-1, -1));
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
    fn test_prune_until() {
        let mut l = create_default_slice();

        assert_eq!(l.size_bytes(), 6);
        assert_eq!(l.next_index(), 6);

        assert!(!l.is_index_compacted(0));
        assert!(!l.is_index_compacted(1));
        assert!(!l.is_index_compacted(2));
        assert!(!l.is_index_compacted(3));
        assert!(!l.is_index_compacted(4));
        assert!(!l.is_index_compacted(5));

        // Prune up to index 3, inclusive.
        l.prune_until(&entry_id(73, 3));
        assert_eq!(l.previous_id, entry_id(73, 3));

        assert_eq!(l.size_bytes(), 2);
        assert_eq!(l.next_index(), 6);

        assert!(l.is_index_compacted(0));
        assert!(l.is_index_compacted(1));
        assert!(l.is_index_compacted(2));
        assert!(l.is_index_compacted(3));
        assert!(!l.is_index_compacted(4));
        assert!(!l.is_index_compacted(5));
    }

    #[test]
    fn test_prune_until_all_entries() {
        {
            let mut l = create_default_slice();
            assert_eq!(l.next_index(), 6);
            assert_eq!(l.size_bytes(), 6);

            l.prune_until(&entry_id(74, 5));
            assert_eq!(l.next_index(), 6);
            assert_eq!(l.size_bytes(), 0);
        }

        {
            let original_previous = entry_id(12, 189);
            let mut l2 = LogSlice::new(original_previous.clone());
            l2.append(12 /* term */, Data::Payload(Vec::new()));
            l2.append(13 /* term */, Data::Payload(Vec::new()));
            assert_eq!(l2.next_index(), 192);

            l2.prune_until(&original_previous);
            assert_eq!(l2.next_index(), 190);
        }
    }

    // Tests that prune_until ends up emptying the slice if the requested
    // stopping entry is not present at all.
    #[test]
    fn test_prune_until_no_match() {
        let mut l = LogSlice::new(EntryId {
            term: 22,
            index: 91,
        });
        assert_eq!(l.next_index(), 92);

        l.append(26 /* term */, Data::Payload(Vec::new()));
        l.append(26 /* term */, Data::Payload(Vec::new()));
        l.append(26 /* term */, Data::Payload(Vec::new()));
        assert_eq!(l.next_index(), 95);

        // Prune until an entry with an index we know about, but the wrong term. Should
        // clear the slice entirely.
        l.prune_until(&EntryId {
            term: 16,
            index: 93,
        });
        assert_eq!(l.size_bytes(), 0);
        assert!(l.entries.is_empty());
        assert_eq!(l.next_index(), 94);
    }

    #[test]
    #[should_panic]
    fn test_append_bad_term() {
        let mut l = create_default_slice();
        l.append(73, Payload("bad term".as_bytes().to_vec()));
    }

    #[test]
    fn test_append() {
        let mut l = create_default_slice();
        let id = l.append(74, Payload("bad term".as_bytes().to_vec()));
        assert_eq!(74, id.term);
        assert_eq!(6, id.index);
    }

    #[test]
    fn test_append_all_from_initial() {
        let mut l = LogSlice::initial();
        l.append_all(&[
            entry(75, 0 /* index */, 10 /* size */),
            entry(75, 1 /* index */, 10 /* size */),
        ]);
        assert_eq!(l.id_at(0), entry_id(75, 0));
        assert_eq!(l.id_at(1), entry_id(75, 1));
    }

    #[test]
    fn test_append_all_from_empty() {
        let mut l = LogSlice::new(entry_id(75, 17 /* index */));
        l.append_all(&[
            entry(75, 18 /* index */, 10 /* size */),
            entry(75, 19 /* index */, 10 /* size */),
        ]);
        assert_eq!(l.id_at(18), entry_id(75, 18));
        assert_eq!(l.id_at(19), entry_id(75, 19));
        assert_eq!(l.next_index(), 20);
    }

    #[test]
    fn test_append_all_replaces_tail() {
        let mut l = create_default_slice();

        // Validate the initial state.
        assert_eq!(l.id_at(0).index, 0);
        assert_eq!(l.id_at(5).index, 5);
        assert_eq!(l.next_index(), 6);
        let initial_size_bytes = 6;
        assert_eq!(l.size_bytes(), initial_size_bytes);

        // This should replace entries 3 and 4, and remove entry 5
        l.append_all(&[
            entry(75, 3 /* index */, 10 /* size */),
            entry(75, 4 /* index */, 10 /* size */),
        ]);
        assert_eq!(l.id_at(0).index, 0);
        assert_eq!(l.next_index(), 5);
        let size_bytes = initial_size_bytes - 3 + 20;
        assert_eq!(l.size_bytes(), size_bytes);

        // This should just append a new entry 5
        l.append_all(&[entry(76, 5 /* index */, 10 /* size */)]);
        assert_eq!(l.next_index(), 6);
        let new_size_bytes = size_bytes + 10;
        assert_eq!(l.size_bytes(), new_size_bytes);
    }

    #[test]
    #[should_panic]
    fn test_append_all_with_empty_entries_panics() {
        let mut l = create_default_slice();
        l.append_all(&[]);
    }

    #[test]
    #[should_panic]
    fn test_append_far_future_index_panics() {
        let mut l = LogSlice::new(entry_id(75, 10));

        // Make an append with indexes that are far in the future.
        l.append_all(&[
            entry(75, 45 /* index */, 10 /* size */),
            entry(75, 46 /* index */, 10 /* size */),
        ]);
    }

    #[test]
    fn test_last_known_id() {
        let initial = LogSlice::initial();
        assert_eq!(initial.last_known_id(), &entry_id(-1, -1));

        let other = LogSlice::new(entry_id(6, 8));
        assert_eq!(other.last_known_id(), &entry_id(6, 8));
    }

    #[test]
    fn test_latest_config_entry() {
        let mut log = create_default_slice();

        assert!(log.latest_config_entry().is_none());
        log.append(
            76, /* term */
            Config(ClusterConfig {
                voters: vec![],
                voters_next: vec![],
            }),
        );

        assert!(log.latest_config_entry().is_some());
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

    fn payload_of_size(size_bytes: i64) -> Data {
        Payload([3].repeat(size_bytes as usize))
    }

    fn entry_id(term: i64, index: i64) -> EntryId {
        EntryId { index, term }
    }

    fn entry(term: i64, index: i64, payload_size_bytes: i64) -> Entry {
        Entry {
            id: Some(entry_id(term, index)),
            data: Some(payload_of_size(payload_size_bytes)),
        }
    }
}
