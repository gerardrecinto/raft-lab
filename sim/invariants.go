package sim

import (
	"fmt"

	"github.com/gerardrecinto/raft-lab/raft"
)

// Invariants checks the safety properties from figure 3 of the Raft paper.
type Invariants struct {
	leaders   map[int]int        // term -> leader id
	committed map[int]raft.Entry // index -> entry some node has committed
	applied   map[int]raft.Entry // index -> entry some node applied
	Violation string
	seen      map[int]int // node -> highest commit index already recorded
	tick      int
}

func NewInvariants() *Invariants {
	return &Invariants{leaders: map[int]int{}, committed: map[int]raft.Entry{}, applied: map[int]raft.Entry{}, seen: map[int]int{}}
}

func (v *Invariants) fail(format string, a ...any) {
	if v.Violation == "" {
		v.Violation = fmt.Sprintf(format, a...)
	}
}

func (v *Invariants) Check(now int64, nodes map[int]*raft.Node, up map[int]bool) {
	if v.Violation != "" {
		return
	}
	for id, n := range nodes {
		if !up[id] {
			continue
		}
		// Election safety: at most one leader per term.
		if n.Role() == raft.Leader {
			if old, ok := v.leaders[n.Term()]; ok && old != id {
				v.fail("t=%d election safety: nodes %d and %d both led term %d", now, old, id, n.Term())
				return
			} else if !ok {
				v.leaders[n.Term()] = id
				// Leader completeness: a new leader holds every committed entry.
				for idx, e := range v.committed {
					log := n.Log()
					if idx > n.LastIndex() || log[idx].Term != e.Term || log[idx].Cmd != e.Cmd {
						v.fail("t=%d leader completeness: node %d became leader of term %d without committed entry %d (term %d, %q)", now, id, n.Term(), idx, e.Term, e.Cmd)
						return
					}
				}
			}
		}
		// Record committed entries (those at or below any node's commit index).
		log := n.Log()
		from := v.seen[id] + 1
		if n.CommitIndex() < v.seen[id] { // node restarted and is re-learning its commit index
			from = n.CommitIndex() + 1
		}
		for i := from; i <= n.CommitIndex() && i < len(log); i++ {
			if old, ok := v.committed[i]; ok && (old.Term != log[i].Term || old.Cmd != log[i].Cmd) {
				v.fail("t=%d state machine safety: index %d committed as (term %d, %q) and (term %d, %q)", now, i, old.Term, old.Cmd, log[i].Term, log[i].Cmd)
				return
			}
			v.committed[i] = log[i]
		}
		if n.CommitIndex() > v.seen[id] {
			v.seen[id] = n.CommitIndex()
		}
	}
	v.tick++
	if v.tick%50 != 0 {
		return
	}
	// Log matching: same index and term means identical prefix.
	ids := make([]int, 0, len(nodes))
	for id := range nodes {
		if up[id] {
			ids = append(ids, id)
		}
	}
	for a := 0; a < len(ids); a++ {
		for b := a + 1; b < len(ids); b++ {
			la, lb := nodes[ids[a]].Log(), nodes[ids[b]].Log()
			lim := len(la)
			if len(lb) < lim {
				lim = len(lb)
			}
			for i := lim - 1; i >= 1; i-- {
				if la[i].Term == lb[i].Term {
					for j := 1; j <= i; j++ {
						if la[j] != lb[j] {
							v.fail("t=%d log matching: nodes %d and %d agree on index %d term %d but differ at index %d", now, ids[a], ids[b], i, la[i].Term, j)
							return
						}
					}
					break
				}
			}
		}
	}
}
