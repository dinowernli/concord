# Concord

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

Sample output:

```
2025-08-24T15:09:20.891004Z  INFO Started http and grpc server [name=A,port=53340]. Open at http://[::1]:53340/keyvalue
2025-08-24T15:09:20.891327Z  INFO Started http and grpc server [name=B,port=53341]. Open at http://[::1]:53341/keyvalue
2025-08-24T15:09:20.891478Z  INFO Started http and grpc server [name=C,port=53342]. Open at http://[::1]:53342/keyvalue
2025-08-24T15:09:20.891625Z  INFO Started http and grpc server [name=D,port=53343]. Open at http://[::1]:53343/keyvalue
2025-08-24T15:09:20.891763Z  INFO Started http and grpc server [name=E,port=53344]. Open at http://[::1]:53344/keyvalue
2025-08-24T15:09:20.892253Z  INFO Started 5 servers
2025-08-24T15:09:22.185730Z  INFO put: success i=1 latency_ms=9
2025-08-24T15:09:24.912587Z  INFO preempt: success leader=C latency_ms=17
2025-08-24T15:09:32.360259Z  INFO put: success i=11 latency_ms=8
2025-08-24T15:09:35.964454Z  INFO reconfigure: success latency_ms=68 new_members=["A", "E", "B"]
2025-08-24T15:09:38.930871Z  INFO preempt: success leader=B latency_ms=14
2025-08-24T15:09:42.599468Z  INFO put: success i=21 latency_ms=49
2025-08-24T15:09:50.990828Z  INFO reconfigure: success latency_ms=23 new_members=["A", "D", "B"]
2025-08-24T15:09:51.204559Z  WARN election{server=A}: vote request failed: RPC error from peer D: status: Unavailable, message: "error injected in channel A -> D", details: [], metadata: MetadataMap { headers: {} }
2025-08-24T15:09:52.724498Z  INFO put: success i=31 latency_ms=7
2025-08-24T15:09:52.949347Z  INFO preempt: success leader=A latency_ms=14
2025-08-24T15:10:02.866367Z  INFO put: success i=41 latency_ms=9
2025-08-24T15:10:06.011490Z  INFO reconfigure: success latency_ms=17 new_members=["A", "E", "D"]
2025-08-24T15:10:06.968440Z  INFO preempt: success leader=D latency_ms=14
^C2025-08-24T15:10:08.541151Z  INFO Got SIGINT, shutting down
2025-08-24T15:10:08.541334Z  INFO put: Finished
2025-08-24T15:10:08.542020Z  INFO preempt: Finished
2025-08-24T15:10:08.542135Z  INFO validate: Finished
2025-08-24T15:10:08.542202Z  INFO reconfigure: Finished
2025-08-24T15:10:08.545322Z  INFO serve{server=A}: Serving terminated
2025-08-24T15:10:08.545471Z  INFO serve{server=B}: Serving terminated
2025-08-24T15:10:08.545553Z  INFO serve{server=C}: Serving terminated
2025-08-24T15:10:08.545626Z  INFO serve{server=D}: Serving terminated
2025-08-24T15:10:08.545699Z  INFO serve{server=E}: Serving terminated
2025-08-24T15:10:08.545746Z  INFO All done, exiting
```

## Test

Standard rust unit tests:

```
> cargo test
```


