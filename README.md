# Concord

[![Build Status](https://travis-ci.org/dinowernli/concord.svg?branch=master)](https://travis-ci.org/dinowernli/concord)

A distributed key-value store in Rust:
* Comes with a grpc-based implementation of Raft.
* Designed to have no external dependencies.
* Uses the Raft library to implement a key-value store.
* Currently a work in progress.

## Run

The main binary starts a cluster with 3 peers and then kicks off multiple periodic actions:
* One thread keeps issuing new commit operations
* One thread keeps asking the leader to step down
* One thread analyzes the history of cluster events in the search of bugs

To run, execute:

```
> cargo run
```

## Test

Standard rust unit tests:

```
> cargo test
```


