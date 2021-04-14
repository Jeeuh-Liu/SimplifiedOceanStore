package pkg

import (
	tapestry "tapestry/pkg"

	"time"
)

// Cluster is an interface for all nodes in a puddlestore cluster. One should be able to shutdown
// this cluster and create a client for this cluster
type Cluster struct {
	config Config
	nodes  []*Tapestry
}

// Shutdown causes all the raft and tapestry nodes to gracefully exit
func (c *Cluster) Shutdown() {
	for _, node := range c.nodes {
		node.GracefulExit()
	}

	time.Sleep(time.Second)
}

// NewClient creates a new Puddlestore client
func (c *Cluster) NewClient() (Client, error) {
	// TODO: Return a new PuddleStore Client that implements the Client interface
	return nil, nil
}

// CreateCluster starts all nodes necessary for puddlestore
func CreateCluster(config Config) (*Cluster, error) {
	// TODO: Start your tapestry cluster with size config.NumTapestry. You should
	// also use the zkAddr (zookeeper address) found in the config and pass it to the
	var c Cluster
	for i := 0; i < config.NumTapestry; i++ {
		connectTo := ""
		if i > 0 {
			connectTo = c.nodes[0].tap.Addr()
		}
		id := tapestry.MakeID(tapestry.RandomID().String())
		t, err := tapestry.Start(id, 0, connectTo)
		if err != nil {
			return &c, err
		}
		tap, err := NewTapestry(t, config.ZkAddr)
		if err != nil {
			return &c, err
		}
		//init client here pass Config.ZkAddr to the client Constructor
		c.nodes = append(c.nodes, tap)
		time.Sleep(10 * time.Millisecond)
	}
	return &c, nil
}
