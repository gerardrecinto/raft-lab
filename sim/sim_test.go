package sim

import (
	"fmt"
	"testing"

	"github.com/gerardrecinto/raft-lab/raft"
)

func mustNoViolation(t *testing.T, c *Cluster, ctx string) {
	t.Helper()
	if c.Inv.Violation != "" {
		t.Fatalf("%s: %s", ctx, c.Inv.Violation)
	}
}

func TestElectsExactlyOneLeader(t *testing.T) {
	c := NewCluster(5, 1, raft.DefaultConfig())
	c.Run(2000)
	mustNoViolation(t, c, "elect")
	leaders := 0
	for i := 0; i < 5; i++ {
		if c.Nodes[i].Role() == raft.Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("want 1 leader, got %d", leaders)
	}
}

func TestReplicatesAndCommitsOnAllNodes(t *testing.T) {
	c := NewCluster(5, 2, raft.DefaultConfig())
	c.Run(1000)
	for i := 0; i < 10; i++ {
		if _, ok := c.Propose(fmt.Sprintf("set k%d", i)); !ok {
			t.Fatal("no leader")
		}
		c.Run(100)
	}
	c.Run(500)
	mustNoViolation(t, c, "replicate")
	for i := 0; i < 5; i++ {
		if got := len(c.Nodes[i].Applied); got < 11 { // 10 commands + leader noop
			t.Fatalf("node %d applied %d entries", i, got)
		}
	}
}

func TestLeaderCrashKeepsCommittedEntries(t *testing.T) {
	c := NewCluster(5, 3, raft.DefaultConfig())
	c.Run(1000)
	for i := 0; i < 5; i++ {
		c.Propose(fmt.Sprintf("a%d", i))
		c.Run(80)
	}
	old := c.Leader()
	c.Crash(old)
	c.Run(1500)
	mustNoViolation(t, c, "crash")
	nl := c.Leader()
	if nl < 0 || nl == old {
		t.Fatalf("no new leader after crash (old %d new %d)", old, nl)
	}
	if _, ok := c.Propose("after-crash"); !ok {
		t.Fatal("new leader refused write")
	}
	c.Run(500)
	c.Restart(old)
	c.Run(1500)
	mustNoViolation(t, c, "restart")
	if len(c.Nodes[old].Applied) != len(c.Nodes[nl].Applied) {
		t.Fatalf("restarted node did not catch up: %d vs %d", len(c.Nodes[old].Applied), len(c.Nodes[nl].Applied))
	}
}

func TestMinorityPartitionCannotCommit(t *testing.T) {
	c := NewCluster(5, 4, raft.DefaultConfig())
	c.Run(1000)
	old := c.Leader()
	minority := []int{old, (old + 1) % 5}
	majority := []int{}
	for i := 0; i < 5; i++ {
		if i != minority[0] && i != minority[1] {
			majority = append(majority, i)
		}
	}
	c.Partition(minority, majority)
	// The stale leader still accepts the write but must not commit it.
	idx, ok, out := c.Nodes[old].Propose(c.Now, "lost-write")
	c.send(out)
	if !ok {
		t.Fatal("old leader already stepped down")
	}
	c.Run(2000)
	mustNoViolation(t, c, "partition")
	if c.Nodes[old].CommitIndex() >= idx {
		t.Fatalf("minority leader committed index %d", idx)
	}
	c.Heal()
	c.Run(2000)
	mustNoViolation(t, c, "heal")
	for i := 0; i < 5; i++ {
		for _, e := range c.Nodes[i].Applied {
			if e.Cmd == "lost-write" {
				t.Fatalf("node %d applied the write that never reached a majority", i)
			}
		}
	}
}

// chaos drives random faults and returns the cluster for inspection.
func chaos(seed int64, cfg raft.Config) *Cluster {
	return chaosMix(seed, cfg, 0.05, 6)
}

// chaosMix: faultBias of 6 means roughly 4 in 10 steps are faults (crash, restart, partition, heal).
func chaosMix(seed int64, cfg raft.Config, drop float64, faultBias int) *Cluster {
	c := NewCluster(5, seed, cfg)
	c.Net.Drop = drop
	r := c.Rand()
	cmd := 0
	for step := 0; step < 400 && c.Inv.Violation == ""; step++ {
		switch r.Intn(faultBias + 4) {
		case 0:
			c.Crash(r.Intn(5))
		case 1:
			c.Restart(r.Intn(5))
		case 2:
			a := r.Perm(5)
			c.Partition(a[:2], a[2:])
		case 3:
			c.Heal()
		default:
			cmd++
			c.Propose(fmt.Sprintf("c%d", cmd))
		}
		c.Run(int64(20 + r.Intn(120)))
	}
	return c
}

func TestChaosKeepsAllInvariants(t *testing.T) {
	for seed := int64(1); seed <= 150; seed++ {
		c := chaos(seed, raft.DefaultConfig())
		if c.Inv.Violation != "" {
			t.Fatalf("seed %d: %s", seed, c.Inv.Violation)
		}
	}
}

func TestChaosThenHealConverges(t *testing.T) {
	for seed := int64(1); seed <= 60; seed++ {
		c := chaos(seed, raft.DefaultConfig())
		c.Net.Drop = 0
		c.Heal()
		for i := 0; i < 5; i++ {
			c.Restart(i)
		}
		c.Run(3000)
		if c.Inv.Violation != "" {
			t.Fatalf("seed %d: %s", seed, c.Inv.Violation)
		}
		if c.Leader() < 0 {
			t.Fatalf("seed %d: no leader after heal", seed)
		}
		if _, ok := c.Propose("final"); !ok {
			t.Fatalf("seed %d: leader rejected write", seed)
		}
		c.Run(1500)
		ref := c.Nodes[0].Applied
		for i := 1; i < 5; i++ {
			a := c.Nodes[i].Applied
			if len(a) != len(ref) {
				t.Fatalf("seed %d: node %d applied %d, node 0 applied %d", seed, i, len(a), len(ref))
			}
			for j := range a {
				if a[j] != ref[j] {
					t.Fatalf("seed %d: node %d diverges at %d", seed, i, j)
				}
			}
		}
	}
}

// The invariant checker is only worth trusting if it fails when the protocol
// is broken. These tests turn off one safety rule and require the chaos run to
// catch it within a bounded number of seeds.
func TestCheckerCatchesMissingElectionRestriction(t *testing.T) {
	cfg := raft.DefaultConfig()
	cfg.UnsafeSkipLogCheckOnVote = true
	for seed := int64(1); seed <= 400; seed++ {
		if c := chaos(seed, cfg); c.Inv.Violation != "" {
			t.Logf("caught on seed %d: %s", seed, c.Inv.Violation)
			return
		}
	}
	t.Fatal("checker never caught a leader missing committed entries in 400 seeds")
}
