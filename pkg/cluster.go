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
	client := &puddleStoreClient{}
	conn, err := ConnectZk(c.config.ZkAddr)
	if err != nil {
		return client, err
	} else {
		client.Conn = conn
		return client, nil
	}
}

// CreateCluster starts all nodes necessary for puddlestore
func CreateCluster(config Config) (*Cluster, error) {
	// TODO: Start your tapestry cluster with size config.NumTapestry.
	//create /tapestry directory
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
		c.nodes = append(c.nodes, tap)
		time.Sleep(10 * time.Millisecond)
	}
	return &c, nil
}
