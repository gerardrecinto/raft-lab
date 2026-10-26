// raftsim runs each scenario and writes the recorded trace as JSON for the
// docs/ viewer. It exits non-zero if any safety invariant is violated.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gerardrecinto/raft-lab/raft"
	"github.com/gerardrecinto/raft-lab/sim"
)

type trace struct {
	Name    string         `json:"name"`
	Title   string         `json:"title"`
	Blurb   string         `json:"blurb"`
	Seed    int64          `json:"seed"`
	Nodes   int            `json:"nodes"`
	EndT    int64          `json:"endT"`
	Events  []sim.Event    `json:"events"`
	Msgs    []sim.MsgTrace `json:"msgs"`
	Applied [][]raft.Entry `json:"applied"`
	Summary summary        `json:"summary"`
}

type summary struct {
	Terms      int    `json:"maxTerm"`
	Elections  int    `json:"leaderChanges"`
	Committed  int    `json:"committedEntries"`
	MsgsSent   int    `json:"messages"`
	Dropped    int    `json:"dropped"`
	Violations string `json:"violations"`
}

func main() {
	out := flag.String("out", "docs/traces", "output directory")
	seed := flag.Int64("seed", 7, "random seed")
	flag.Parse()
	_ = os.MkdirAll(*out, 0o755)
	var index []map[string]string
	bad := false
	for _, s := range sim.Scenarios() {
		c := sim.NewScenarioCluster(*seed)
		s.Run(c)
		t := trace{Name: s.Name, Title: s.Title, Blurb: s.Blurb, Seed: *seed, Nodes: c.N, EndT: c.Now, Events: c.Events, Msgs: c.Msgs}
		maxTerm, leaders := 0, 0
		for _, e := range c.Events {
			if e.Term > maxTerm {
				maxTerm = e.Term
			}
			if e.Kind == "role" && e.Role == "leader" {
				leaders++
			}
		}
		committed := 0
		for i := 0; i < c.N; i++ {
			t.Applied = append(t.Applied, c.Nodes[i].Applied)
			if n := len(c.Nodes[i].Applied); n > committed {
				committed = n
			}
		}
		dropped := 0
		for _, m := range c.Msgs {
			if m.Dropped {
				dropped++
			}
		}
		t.Summary = summary{maxTerm, leaders, committed, len(c.Msgs), dropped, c.Inv.Violation}
		if c.Inv.Violation != "" {
			bad = true
		}
		b, _ := json.Marshal(t)
		_ = os.WriteFile(filepath.Join(*out, s.Name+".json"), b, 0o644)
		index = append(index, map[string]string{"name": s.Name, "title": s.Title})
		fmt.Printf("%-14s t=%dms leaders=%d maxTerm=%d committed=%d msgs=%d dropped=%d violation=%q\n", s.Name, c.Now, leaders, maxTerm, committed, len(c.Msgs), dropped, c.Inv.Violation)
	}
	b, _ := json.Marshal(index)
	_ = os.WriteFile(filepath.Join(*out, "index.json"), b, 0o644)
	if bad {
		os.Exit(1)
	}
}
