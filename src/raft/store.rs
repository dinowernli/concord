
// Handles persistent storage for a Raft member, including a snapshot starting
// from the beginning of time up to some index, and running log of entries since
// the index included in the last snapshot.
//
// Also takes care of periodic compaction, updating listeners when the committed
// portion reaches as certain index, etc.
struct Store {
    
}