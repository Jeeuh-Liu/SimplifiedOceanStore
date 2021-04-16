package pkg

import (
	"fmt"
	tapestry "tapestry/pkg"
	"time"

	"github.com/samuel/go-zookeeper/zk"
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
	conn, err := ConnectZk(DefaultConfig().ZkAddr)
	if err != nil {
		return nil, fmt.Errorf("%v, %v", c.config.ZkAddr, err)
	}
	if err == nil {
		client.Conn = conn
		client.init()
		// client.connectAll()
		go client.watch()
	}
	return client, err
}

// CreateCluster starts all nodes necessary for puddlestore
func CreateCluster(config Config) (*Cluster, error) {
	//create tapestry directory file
	zkConn, err := ConnectZk(config.ZkAddr)
	if err != nil {
		return nil, err
	}
	path := "/tapestry"
	data := []byte("tapestry directory")
	_, err = zkConn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return nil, err
	}
	// TODO: Start your tapestry cluster with size config.NumTapestry.
	var c Cluster
	c.config.ZkAddr = DefaultConfig().ZkAddr
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
