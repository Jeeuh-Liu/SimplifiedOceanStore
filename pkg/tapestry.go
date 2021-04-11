package pkg

import (
	"github.com/samuel/go-zookeeper/zk"
	tapestry "tapestry/pkg"
)

// Tapestry is a wrapper for a single Tapestry node. It is responsible for
// maintaining a zookeeper connection and implementing methods we provide
type Tapestry struct {
	tap *tapestry.Node
	zk  *zk.Conn
}

func NewTapestry(tap *tapestry.Node, zkAddr string) (*Tapestry, error) {
	//  TODO: Setup a zookeeper connection and return a Tapestry struct
	return nil, nil
}

// GracefulExit closes the zookeeper connection and gracefully shuts down the tapestry node
func (t *Tapestry) GracefulExit() {
	t.zk.Close()
	t.tap.Leave()
}
