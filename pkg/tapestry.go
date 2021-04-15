package pkg

import (
	tapestry "tapestry/pkg"

	"github.com/samuel/go-zookeeper/zk"
)

// Tapestry is a wrapper for a single Tapestry node. It is responsible for
// maintaining a zookeeper connection and implementing methods we provide
type Tapestry struct {
	tap *tapestry.Node
	zk  *zk.Conn
}

func NewTapestry(tap *tapestry.Node, zkAddr string) (*Tapestry, error) {
	//  TODO: Setup a zookeeper connection and return a Tapestry struct
	zkConn, err := ConnectZk(zkAddr)
	if err != nil {
		return nil, err
	}
	Tap := Tapestry{tap: tap, zk: zkConn}
	//use that session to create an Ephemeral file, then when the node fails the session will also disappear.
	//Clients can then watch these znodes to determine which tapestry nodes are active or not.
	//so you can use zk watches to track your membership changes
	//each Tapestry node to make a file within /tapestry/, do not know whether the implementation is correct
	path := "/tapestry/node-000" + Tap.tap.ID()
	data := []byte(Tap.tap.Node.Address)
	_, err = CreateEphSeq(zkConn, path, data)
	if err != nil {
		return nil, err
	}
	return &Tap, nil
}

// GracefulExit closes the zookeeper connection and gracefully shuts down the tapestry node
func (t *Tapestry) GracefulExit() {
	t.zk.Close()
	t.tap.Leave()
}
