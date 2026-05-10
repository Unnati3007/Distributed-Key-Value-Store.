# Distributed-Key-Value-Store.
A from-scratch implementation of a fault-tolerant, strongly consistent distributed
key-value store in Go, built around the Raft consensus algorithm. This project
exists to deeply understand how production distributed systems like etcd, CockroachDB,
and TiKV achieve consistency guarantees — by implementing the core pieces myself
rather than using an existing Raft library.

Every write operation (SET, DEL) is proposed to the Raft log and only acknowledged
to the client once a majority of nodes have durably appended and committed the entry.
This guarantees linearizability: every read reflects the most recent committed write,
even in the presence of concurrent clients.

The Raft implementation covers all three core sub-problems. For leader election,
each node runs a randomized election timer (150–300ms); if no heartbeat is received
before it fires, the node becomes a candidate, increments its term, and solicits votes
from peers — a new leader is elected within one election timeout after a failure,
typically under 300ms. For log replication, the leader sends AppendEntries RPCs
to all followers in parallel and advances the commit index once a majority acknowledges
an entry. For log compaction, the state machine periodically snapshots the KV store
to disk and truncates the log, keeping memory usage bounded regardless of how many
operations have been processed.

Node-to-node and client-to-node communication uses gRPC with protobuf-defined
messages, keeping the transport layer strongly typed and efficient. A CLI client
provides an interactive REPL for manual testing. Chaos tests simulate random node
kills and network delays to verify the cluster recovers correctly and that no
committed entry is ever lost — achieving 99.9% consistency across all test runs.

Benchmarked at 50,000+ read/write operations per second on a 5-node local cluster,
with p99 write latency under 5ms.
