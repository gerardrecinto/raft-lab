package raft

// Role is the state of a node in the protocol.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	}
	return "leader"
}

// Entry is one slot in the replicated log. Index 0 is a sentinel.
type Entry struct {
	Term  int    `json:"term"`
	Index int    `json:"index"`
	Cmd   string `json:"cmd"`
}

// Noop is appended by a new leader so entries from earlier terms can commit
// without waiting for a client write.
const Noop = "noop"

// Persistent is the state a real node must write to disk before replying.
// The simulator keeps it across crashes and discards everything else.
type Persistent struct {
	Term     int     `json:"term"`
	VotedFor int     `json:"votedFor"`
	Log      []Entry `json:"log"`
}

func (p Persistent) Clone() Persistent {
	l := make([]Entry, len(p.Log))
	copy(l, p.Log)
	return Persistent{Term: p.Term, VotedFor: p.VotedFor, Log: l}
}

// Config holds timing in virtual milliseconds.
type Config struct {
	ElectionMin int64
	ElectionMax int64
	Heartbeat   int64
	MaxBatch    int

	// Unsafe switches exist only so the tests can prove the invariant
	// checker catches real protocol bugs. Never set them in a real node.
	UnsafeSkipLogCheckOnVote bool
}

func DefaultConfig() Config {
	return Config{ElectionMin: 150, ElectionMax: 300, Heartbeat: 50, MaxBatch: 64}
}

type Msg struct {
	From int
	To   int
	Body any
}

type RequestVote struct {
	Term         int
	Candidate    int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term    int
	Granted bool
}

type AppendEntries struct {
	Term         int
	Leader       int
	PrevIndex    int
	PrevTerm     int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	MatchIndex    int
	ConflictTerm  int
	ConflictIndex int
}
