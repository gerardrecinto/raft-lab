// Package sim runs Raft nodes on a virtual clock with a lossy network,
// crashes and partitions, and checks the Raft safety properties after every
// millisecond of simulated time.
package sim

import (
	"container/heap"
	"fmt"
	"math/rand"

	"github.com/gerardrecinto/raft-lab/raft"
)

type Event struct {
	T      int64  `json:"t"`
	Kind   string `json:"kind"` // role, commit, propose, crash, restart, partition, heal, note
	Node   int    `json:"node"`
	Term   int    `json:"term,omitempty"`
	Role   string `json:"role,omitempty"`
	Commit int    `json:"commit,omitempty"`
	Index  int    `json:"index,omitempty"`
	Text   string `json:"text,omitempty"`
}

type MsgTrace struct {
	T       int64  `json:"t"`
	Deliver int64  `json:"deliver"`
	From    int    `json:"from"`
	To      int    `json:"to"`
	Kind    string `json:"kind"`
	Dropped bool   `json:"dropped,omitempty"`
}

type inflight struct {
	at  int64
	seq int
	msg raft.Msg
}

type queue []inflight

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q queue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(x any)   { *q = append(*q, x.(inflight)) }
func (q *queue) Pop() any {
	old := *q
	v := old[len(old)-1]
	*q = old[:len(old)-1]
	return v
}

type Network struct {
	MinLat, MaxLat int64
	Drop           float64
}

type Cluster struct {
	Now   int64
	N     int
	Cfg   raft.Config
	Net   Network
	Nodes map[int]*raft.Node
	Up    map[int]bool

	saved map[int]raft.Persistent
	group map[int]int // partition group per node, all 0 when healed
	q     queue
	seq   int
	rng   *rand.Rand
	seed  int64
	boots int

	Events []Event
	Msgs   []MsgTrace
	Trace  bool
	Inv    *Invariants

	prev map[int]snap
}

type snap struct {
	role   raft.Role
	term   int
	commit int
}

func NewCluster(n int, seed int64, cfg raft.Config) *Cluster {
	c := &Cluster{
		N: n, Cfg: cfg, Net: Network{MinLat: 5, MaxLat: 25},
		Nodes: map[int]*raft.Node{}, Up: map[int]bool{},
		saved: map[int]raft.Persistent{}, group: map[int]int{},
		rng: rand.New(rand.NewSource(seed)), seed: seed, prev: map[int]snap{},
	}
	c.Inv = NewInvariants()
	for i := 0; i < n; i++ {
		c.boot(i, nil)
	}
	return c
}

func (c *Cluster) peersOf(id int) []int {
	var p []int
	for i := 0; i < c.N; i++ {
		if i != id {
			p = append(p, i)
		}
	}
	return p
}

func (c *Cluster) boot(id int, ps *raft.Persistent) {
	c.boots++
	c.Nodes[id] = raft.New(id, c.peersOf(id), c.Cfg, c.seed*1000+int64(id)*37+int64(c.boots), ps, c.Now)
	c.Up[id] = true
	c.prev[id] = snap{role: raft.Follower, term: c.Nodes[id].Term()}
}

func (c *Cluster) event(e Event) {
	e.T = c.Now
	if c.Trace {
		c.Events = append(c.Events, e)
	}
}

func (c *Cluster) send(msgs []raft.Msg) {
	for _, m := range msgs {
		lat := c.Net.MinLat + c.rng.Int63n(c.Net.MaxLat-c.Net.MinLat+1)
		dropped := c.group[m.From] != c.group[m.To] || c.rng.Float64() < c.Net.Drop
		if c.Trace {
			c.Msgs = append(c.Msgs, MsgTrace{T: c.Now, Deliver: c.Now + lat, From: m.From, To: m.To, Kind: kindOf(m.Body), Dropped: dropped})
		}
		if dropped {
			continue
		}
		c.seq++
		heap.Push(&c.q, inflight{at: c.Now + lat, seq: c.seq, msg: m})
	}
}

func kindOf(b any) string {
	switch b.(type) {
	case raft.RequestVote:
		return "vote"
	case raft.RequestVoteReply:
		return "voteReply"
	case raft.AppendEntries:
		return "append"
	}
	return "appendReply"
}

// Step advances one virtual millisecond.
func (c *Cluster) Step() {
	c.Now++
	for c.q.Len() > 0 && c.q[0].at <= c.Now {
		it := heap.Pop(&c.q).(inflight)
		// A partition raised after send still cuts messages in flight.
		if !c.Up[it.msg.To] || c.group[it.msg.From] != c.group[it.msg.To] {
			continue
		}
		c.send(c.Nodes[it.msg.To].Step(c.Now, it.msg))
	}
	for i := 0; i < c.N; i++ {
		if c.Up[i] {
			c.send(c.Nodes[i].Tick(c.Now))
		}
	}
	c.observe()
}

func (c *Cluster) observe() {
	for i := 0; i < c.N; i++ {
		if !c.Up[i] {
			continue
		}
		n := c.Nodes[i]
		p := c.prev[i]
		if n.Role() != p.role || n.Term() != p.term {
			c.event(Event{Kind: "role", Node: i, Term: n.Term(), Role: n.Role().String()})
		}
		if n.CommitIndex() != p.commit {
			c.event(Event{Kind: "commit", Node: i, Term: n.Term(), Commit: n.CommitIndex()})
		}
		c.prev[i] = snap{n.Role(), n.Term(), n.CommitIndex()}
	}
	c.Inv.Check(c.Now, c.Nodes, c.Up)
}

func (c *Cluster) Run(ms int64) {
	end := c.Now + ms
	for c.Now < end {
		c.Step()
	}
}

// Leader returns the up node that is leader in the highest term, or -1.
func (c *Cluster) Leader() int {
	best, bt := -1, -1
	for i := 0; i < c.N; i++ {
		if c.Up[i] && c.Nodes[i].Role() == raft.Leader && c.Nodes[i].Term() > bt {
			best, bt = i, c.Nodes[i].Term()
		}
	}
	return best
}

// Propose sends a command to whoever is leader. Returns whether a leader took it.
func (c *Cluster) Propose(cmd string) (int, bool) {
	l := c.Leader()
	if l < 0 {
		return 0, false
	}
	idx, ok, out := c.Nodes[l].Propose(c.Now, cmd)
	if !ok {
		return 0, false
	}
	c.event(Event{Kind: "propose", Node: l, Term: c.Nodes[l].Term(), Index: idx, Text: cmd})
	c.send(out)
	return idx, true
}

func (c *Cluster) Crash(id int) {
	if !c.Up[id] {
		return
	}
	c.saved[id] = c.Nodes[id].Durable()
	c.Up[id] = false
	c.event(Event{Kind: "crash", Node: id})
}

func (c *Cluster) Restart(id int) {
	if c.Up[id] {
		return
	}
	ps := c.saved[id]
	c.boot(id, &ps)
	c.event(Event{Kind: "restart", Node: id})
}

// Partition splits the cluster; nodes in a map value group can talk to each other.
func (c *Cluster) Partition(groups ...[]int) {
	for i := 0; i < c.N; i++ {
		c.group[i] = 0
	}
	for g, ids := range groups {
		for _, id := range ids {
			c.group[id] = g + 1
		}
	}
	c.event(Event{Kind: "partition", Node: -1, Text: fmt.Sprint(groups)})
}

func (c *Cluster) Heal() {
	for i := 0; i < c.N; i++ {
		c.group[i] = 0
	}
	c.event(Event{Kind: "heal", Node: -1})
}

func (c *Cluster) Note(s string) { c.event(Event{Kind: "note", Node: -1, Text: s}) }

// Rand exposes the cluster's seeded generator to scenario scripts.
func (c *Cluster) Rand() *rand.Rand { return c.rng }
