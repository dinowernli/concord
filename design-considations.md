# Persitence

## Reliability model

* Entry is committed once a majority have appended to its log
* Actually bumping the "commited index" is just bookkeeping
* No out-of-consensus member changes
  * Need quorum to change membership
  * Need quorum to remove permanently dead member
  
Machines can suffer from:
* Temporary failures (crashes, network partitions)
* Permanent failures (disk wiped, permanent machine loss)

Raft guarantees availability as long a strict majority (quorum) of nodes is operational. This typically 
requires most temporary failures to be remedied quickly (network restored, crashes restarted), and 
permanent failures to swiftly get machines replaced. 

Raft is fundamentally not designed to have ephemeral-only logs. In an implementation with epehmeral-only,
losing a majority of nodes (e.g., to a crash) would result in permanent data loss because a majority of
the cluster can no longer determine whether a majority of nodes had the entries in their logs.

## Implementation notes from paper

* Persist currentTerm, votedFor, and log entries
* Persist synchronously whenever they change
* For log entries
  * Either append => append proto to file
  * Replace (rare) => just overwrite entire file
* Disk failure
  * Only send or respond to next RPC once successfully persisted
  * Either need special mode, or just panic on disk failure

## Implementation notes for Concord

* Directory for each consensus node
* Contains
  * One file with proto(term, voted)
  * One file with size-delimited entry protos
* Flush immediately after change
* Persistent state is spread across components
  * Log (LogSlice), and term/voted (RaftStateImpl)
  * Interactions with LogSlice don't go through Store (dangerous)

# Linearizability + Serializability

* Check that when requesting the commit for an entry, the result committed is actually the entry we requested rather
than someone else having received that index.
  * We do a EntryId(term,index) match check in commit_internal, but not sure if sufficient
  * The failure mode is losing leadership between when we appended the entry and when the index got committed (so the index we're waiting for could be a different entry)
  * But this should be safe because we check the term (and any other leader must be in a different term)
* Exactly-once: Check that client-side timeout when writing is handled correctly. Covered in an extension in the Raft paper - need to keep track of (client, monotonic-id) in the raft log entry so that other participants can detect duplicates

# Fault injection

* Add support for delaying response (in addition to delaying request)