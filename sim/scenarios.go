package sim

import (
	"fmt"

	"github.com/gerardrecinto/raft-lab/raft"
)

type Scenario struct {
	Name  string
	Title string
	Blurb string
	Run   func(c *Cluster)
}

func settle(c *Cluster, ms int64) { c.Run(ms) }

func writes(c *Cluster, prefix string, n int, gap int64) {
	for i := 0; i < n; i++ {
		c.Propose(fmt.Sprintf("%s%d", prefix, i))
		c.Run(gap)
	}
}

// Scenarios are scripted fault sequences. The viewer replays the exact trace
// the simulator recorded, nothing is drawn by hand.
func Scenarios() []Scenario {
	return []Scenario{
		{
			Name: "election", Title: "Cold start election",
			Blurb: "Five nodes start as followers. The first election timer to fire wins a majority and starts sending heartbeats.",
			Run: func(c *Cluster) {
				c.Note("5 nodes boot as followers")
				c.Run(1500)
				writes(c, "w", 3, 150)
				c.Run(500)
			},
		},
		{
			Name: "leader-crash", Title: "Leader crash and recovery",
			Blurb: "Commit some writes, kill the leader, watch a new leader win, keep writing, then restart the old leader and watch it catch up from its saved log.",
			Run: func(c *Cluster) {
				c.Run(1200)
				writes(c, "a", 4, 120)
				old := c.Leader()
				c.Note(fmt.Sprintf("crashing leader %d", old))
				c.Crash(old)
				c.Run(1500)
				writes(c, "b", 4, 120)
				c.Note(fmt.Sprintf("restarting node %d", old))
				c.Restart(old)
				c.Run(1500)
			},
		},
		{
			Name: "partition", Title: "Minority partition cannot commit",
			Blurb: "Cut the leader and one follower off from the other three. The old leader accepts a write it can never commit. The majority elects a new leader and moves on. After healing, the stale write is overwritten.",
			Run: func(c *Cluster) {
				c.Run(1200)
				writes(c, "a", 3, 120)
				old := c.Leader()
				minor := []int{old, (old + 1) % 5}
				var major []int
				for i := 0; i < 5; i++ {
					if i != minor[0] && i != minor[1] {
						major = append(major, i)
					}
				}
				c.Partition(minor, major)
				_, _, out := c.Nodes[old].Propose(c.Now, "stale-write")
				c.send(out)
				c.event(Event{Kind: "propose", Node: old, Term: c.Nodes[old].Term(), Index: c.Nodes[old].LastIndex(), Text: "stale-write (minority)"})
				c.Run(1800)
				writes(c, "m", 3, 120)
				c.Note("healing the partition")
				c.Heal()
				c.Run(2000)
			},
		},
		{
			Name: "chaos", Title: "Random faults, invariants checked",
			Blurb: "Seeded random crashes, restarts, partitions and 5% message loss while clients keep writing. The safety checker runs every simulated millisecond.",
			Run: func(c *Cluster) {
				c.Net.Drop = 0.05
				r := c.Rand()
				cmd := 0
				for step := 0; step < 40; step++ {
					switch r.Intn(10) {
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
				c.Net.Drop = 0
				c.Heal()
				for i := 0; i < 5; i++ {
					c.Restart(i)
				}
				c.Run(2500)
			},
		},
	}
}

// NewScenarioCluster is the single place scenario seeds are chosen.
func NewScenarioCluster(seed int64) *Cluster {
	c := NewCluster(5, seed, raft.DefaultConfig())
	c.Trace = true
	return c
}
