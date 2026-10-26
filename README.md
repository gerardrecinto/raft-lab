# raft-lab

A from-scratch implementation of the Raft consensus algorithm in Go, run on a deterministic virtual clock with fault injection and a safety checker.

**Live demo:** https://gerardrecinto.github.io/raft-lab/ replays traces recorded by this code: a cold-start election, a leader crash and recovery, a minority partition that cannot commit, and a seeded chaos run.

This is a personal learning project. It is not a production system and it has not run on real machines or a real network.

## Why it exists

Raft is the consensus protocol behind etcd and therefore behind Kubernetes control planes. Running HA clusters taught me what quorum loss and leader election look like from the outside. This repo is the inside view: implement the protocol from the paper, then try to break it.

## What is implemented

- Leader election with randomized election timeouts
- Log replication with the consistency check from figure 2 of the paper
- The commit rule from section 5.4.2: a leader only counts replicas for entries from its own term
- Fast log backtracking using a conflict term and index hint
- A no-op entry appended by each new leader so older entries can commit
- Durable term, vote and log that survive a simulated crash; everything else is rebuilt on restart

Not implemented: snapshots and log compaction, membership changes, read-only queries (read index or leases), a real network, a real disk.

## Design

A `raft.Node` never sleeps, starts goroutines or opens sockets. The caller feeds it `Tick(now)` and `Step(now, msg)` and collects the messages it wants sent. The `sim` package owns a virtual clock, a priority queue of in-flight messages with random latency and loss, node crashes, restarts and network partitions. Because time and randomness are both seeded, any failing run replays exactly.

After every simulated millisecond `sim.Invariants` checks the four safety properties from figure 3 of the paper:

| Property | Check |
|---|---|
| Election safety | at most one leader per term |
| Log matching | if two logs share an index and term, they are identical up to that index |
| State machine safety | no two nodes ever commit different entries at the same index |
| Leader completeness | a new leader's log contains every entry committed so far |

## Results

```
go test ./... -race
```

- Four scenario tests: one leader elected, ten writes commit on all five nodes, committed writes survive a leader crash and the old leader catches up, a minority partition cannot commit.
- 150 seeded chaos runs (random crashes, restarts, partitions, 5% message loss) with no invariant violation.
- 60 chaos runs followed by a heal where every node must converge to the same applied log and accept a new write.
- A mutation test: with the vote-time log check disabled (`UnsafeSkipLogCheckOnVote`), the checker reports a leader that is missing a committed entry on the first seed (`leader completeness: node 4 became leader of term 26 without committed entry 21`). This shows the checker fails when the protocol is wrong.

## What did not work

I also tried to catch a second mutation, committing old-term entries by counting replicas (the figure 8 hazard from the paper). 3,400 random seeds, including heavier fault mixes, never exposed it, so I removed that test instead of keeping one that skips. My guess is that the leader no-op masks most of the interleavings that figure 8 needs, but I did not prove that. A hand-built figure 8 schedule would be the right test and is not written.

## Run it

```
go test ./... -race -count=1
go run ./cmd/raftsim           # writes docs/traces/*.json, exits 1 on any violation
python3 -m http.server -d docs # then open http://localhost:8000
```

CI runs the tests with the race detector and fails if the committed traces differ from what the code produces.

## References

- Diego Ongaro and John Ousterhout, [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) (USENIX ATC 2014). Figures 2 and 3 are the spec and the safety properties used here.
- Diego Ongaro, [Consensus: Bridging Theory and Practice](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf) (PhD thesis, 2014). Source for log backtracking and the leader no-op.
- [raft.github.io](https://raft.github.io/), the official Raft site and visualization.
- [etcd documentation](https://etcd.io/docs/latest/learning/design-learner/), a production Raft system.
- [Apache Kafka: replication](https://kafka.apache.org/documentation/#replication), for contrast between quorum consensus and in-sync-replica replication.
