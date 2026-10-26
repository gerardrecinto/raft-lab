// Package raft is a small Raft implementation driven by a virtual clock.
// A Node never sleeps, spawns goroutines or touches the network: callers feed
// it ticks and messages and collect the messages it wants to send. That makes
// every run deterministic for a given seed, which is what lets the simulator
// replay a failure exactly.
//
// Implemented: leader election, log replication, the commit rule from
// section 5.4.2, fast log backtracking, persistence of term, vote and log.
// Not implemented: snapshots, membership changes, read-only queries.
package raft

import "math/rand"

type Node struct {
	ID    int
	Peers []int // every other node id

	cfg Config
	rng *rand.Rand
	ps  Persistent

	role     Role
	leaderID int
	commit   int
	applied  int

	nextIdx  map[int]int
	matchIdx map[int]int
	votes    map[int]bool

	electionAt  int64
	heartbeatAt int64

	// Applied is the volatile state machine: commands in commit order.
	Applied []Entry
}

// New builds a node from durable state (zero value means a fresh node).
func New(id int, peers []int, cfg Config, seed int64, ps *Persistent, now int64) *Node {
	n := &Node{ID: id, Peers: peers, cfg: cfg, rng: rand.New(rand.NewSource(seed)), leaderID: -1}
	if ps != nil {
		n.ps = ps.Clone()
	} else {
		n.ps = Persistent{VotedFor: -1, Log: []Entry{{}}}
	}
	n.resetElection(now)
	return n
}

func (n *Node) Role() Role          { return n.role }
func (n *Node) Term() int           { return n.ps.Term }
func (n *Node) CommitIndex() int    { return n.commit }
func (n *Node) LeaderID() int       { return n.leaderID }
func (n *Node) Durable() Persistent { return n.ps.Clone() }
func (n *Node) LastIndex() int      { return len(n.ps.Log) - 1 }
func (n *Node) Log() []Entry        { return n.ps.Log }
func (n *Node) lastTerm() int       { return n.ps.Log[n.LastIndex()].Term }
func (n *Node) majority() int       { return (len(n.Peers)+1)/2 + 1 }
func (n *Node) resetElection(now int64) {
	span := n.cfg.ElectionMax - n.cfg.ElectionMin
	n.electionAt = now + n.cfg.ElectionMin + n.rng.Int63n(span+1)
}

func (n *Node) becomeFollower(term int, now int64) {
	if term > n.ps.Term {
		n.ps.Term = term
		n.ps.VotedFor = -1
	}
	n.role = Follower
}

// Tick advances timers. Returns messages to send.
func (n *Node) Tick(now int64) []Msg {
	if n.role == Leader {
		if now >= n.heartbeatAt {
			n.heartbeatAt = now + n.cfg.Heartbeat
			return n.broadcastAppend()
		}
		return nil
	}
	if now >= n.electionAt {
		return n.startElection(now)
	}
	return nil
}

func (n *Node) startElection(now int64) []Msg {
	n.ps.Term++
	n.role = Candidate
	n.ps.VotedFor = n.ID
	n.leaderID = -1
	n.votes = map[int]bool{n.ID: true}
	n.resetElection(now)
	if len(n.votes) >= n.majority() { // single-node cluster
		return n.becomeLeader(now)
	}
	var out []Msg
	for _, p := range n.Peers {
		out = append(out, Msg{n.ID, p, RequestVote{n.ps.Term, n.ID, n.LastIndex(), n.lastTerm()}})
	}
	return out
}

func (n *Node) becomeLeader(now int64) []Msg {
	n.role = Leader
	n.leaderID = n.ID
	n.nextIdx = map[int]int{}
	n.matchIdx = map[int]int{}
	for _, p := range n.Peers {
		n.nextIdx[p] = n.LastIndex() + 1
		n.matchIdx[p] = 0
	}
	n.ps.Log = append(n.ps.Log, Entry{Term: n.ps.Term, Index: n.LastIndex() + 1, Cmd: Noop})
	n.heartbeatAt = now + n.cfg.Heartbeat
	n.advanceCommit()
	return n.broadcastAppend()
}

// Propose appends a client command if this node is the leader.
func (n *Node) Propose(now int64, cmd string) (index int, ok bool, out []Msg) {
	if n.role != Leader {
		return 0, false, nil
	}
	n.ps.Log = append(n.ps.Log, Entry{Term: n.ps.Term, Index: n.LastIndex() + 1, Cmd: cmd})
	n.advanceCommit()
	return n.LastIndex(), true, n.broadcastAppend()
}

func (n *Node) broadcastAppend() []Msg {
	var out []Msg
	for _, p := range n.Peers {
		out = append(out, n.appendFor(p))
	}
	return out
}

func (n *Node) appendFor(p int) Msg {
	next := n.nextIdx[p]
	prev := next - 1
	end := n.LastIndex()
	if n.cfg.MaxBatch > 0 && end-prev > n.cfg.MaxBatch {
		end = prev + n.cfg.MaxBatch
	}
	ents := make([]Entry, end-prev)
	copy(ents, n.ps.Log[next:end+1])
	return Msg{n.ID, p, AppendEntries{n.ps.Term, n.ID, prev, n.ps.Log[prev].Term, ents, n.commit}}
}

// Step handles one incoming message.
func (n *Node) Step(now int64, m Msg) []Msg {
	switch b := m.Body.(type) {
	case RequestVote:
		return n.onRequestVote(now, m.From, b)
	case RequestVoteReply:
		return n.onVoteReply(now, m.From, b)
	case AppendEntries:
		return n.onAppend(now, m.From, b)
	case AppendEntriesReply:
		return n.onAppendReply(now, m.From, b)
	}
	return nil
}

func (n *Node) onRequestVote(now int64, from int, r RequestVote) []Msg {
	if r.Term > n.ps.Term {
		n.becomeFollower(r.Term, now)
	}
	grant := false
	if r.Term == n.ps.Term && (n.ps.VotedFor == -1 || n.ps.VotedFor == r.Candidate) {
		upToDate := r.LastLogTerm > n.lastTerm() ||
			(r.LastLogTerm == n.lastTerm() && r.LastLogIndex >= n.LastIndex())
		if upToDate || n.cfg.UnsafeSkipLogCheckOnVote {
			grant = true
			n.ps.VotedFor = r.Candidate
			n.resetElection(now)
		}
	}
	return []Msg{{n.ID, from, RequestVoteReply{n.ps.Term, grant}}}
}

func (n *Node) onVoteReply(now int64, from int, r RequestVoteReply) []Msg {
	if r.Term > n.ps.Term {
		n.becomeFollower(r.Term, now)
		return nil
	}
	if n.role != Candidate || r.Term != n.ps.Term || !r.Granted {
		return nil
	}
	n.votes[from] = true
	if len(n.votes) >= n.majority() {
		return n.becomeLeader(now)
	}
	return nil
}

func (n *Node) onAppend(now int64, from int, a AppendEntries) []Msg {
	reply := func(ok bool, match, ct, ci int) []Msg {
		return []Msg{{n.ID, from, AppendEntriesReply{n.ps.Term, ok, match, ct, ci}}}
	}
	if a.Term < n.ps.Term {
		return reply(false, 0, 0, 0)
	}
	n.becomeFollower(a.Term, now) // also steps a candidate down in the same term
	n.leaderID = a.Leader
	n.resetElection(now)

	if a.PrevIndex > n.LastIndex() {
		return reply(false, 0, 0, n.LastIndex()+1)
	}
	if t := n.ps.Log[a.PrevIndex].Term; t != a.PrevTerm {
		ci := a.PrevIndex
		for ci > 1 && n.ps.Log[ci-1].Term == t {
			ci--
		}
		return reply(false, 0, t, ci)
	}
	for _, e := range a.Entries {
		if e.Index <= n.LastIndex() {
			if n.ps.Log[e.Index].Term == e.Term {
				continue
			}
			n.ps.Log = n.ps.Log[:e.Index] // conflict: drop this entry and everything after
		}
		n.ps.Log = append(n.ps.Log, e)
	}
	match := a.PrevIndex + len(a.Entries)
	if a.LeaderCommit > n.commit {
		c := a.LeaderCommit
		if match < c {
			c = match
		}
		if c > n.commit {
			n.commit = c
			n.apply()
		}
	}
	return reply(true, match, 0, 0)
}

func (n *Node) onAppendReply(now int64, from int, r AppendEntriesReply) []Msg {
	if r.Term > n.ps.Term {
		n.becomeFollower(r.Term, now)
		return nil
	}
	if n.role != Leader || r.Term != n.ps.Term {
		return nil
	}
	if r.Success {
		if r.MatchIndex > n.matchIdx[from] {
			n.matchIdx[from] = r.MatchIndex
		}
		n.nextIdx[from] = n.matchIdx[from] + 1
		n.advanceCommit()
		return nil
	}
	// Fast backtrack: jump to the conflicting term instead of one index at a time.
	next := r.ConflictIndex
	if r.ConflictTerm > 0 {
		for i := n.LastIndex(); i >= 1; i-- {
			if n.ps.Log[i].Term == r.ConflictTerm {
				next = i + 1
				break
			}
		}
	}
	if next < 1 {
		next = 1
	}
	n.nextIdx[from] = next
	return []Msg{n.appendFor(from)}
}

func (n *Node) advanceCommit() {
	if len(n.Peers) == 0 {
		n.commit = n.LastIndex()
		n.apply()
		return
	}
	for idx := n.LastIndex(); idx > n.commit; idx-- {
		// Section 5.4.2: only count replicas for entries of the current term.
		if n.ps.Log[idx].Term != n.ps.Term {
			break
		}
		count := 1
		for _, p := range n.Peers {
			if n.matchIdx[p] >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commit = idx
			n.apply()
			return
		}
	}
}

func (n *Node) apply() {
	for n.applied < n.commit {
		n.applied++
		n.Applied = append(n.Applied, n.ps.Log[n.applied])
	}
}
