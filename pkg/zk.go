package pkg

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

// connectZk sets up the zookeeper connection
func ConnectZk(zkAddr string) (*zk.Conn, error) {
	conn, _, err := zk.Connect([]string{zkAddr}, 1*time.Second)
	return conn, err
}

// createEphSeq sets up the zookeeper connection and creates an ephemeral znode
func CreateEphSeq(conn *zk.Conn, path string, data []byte) (*zk.Conn, error) {
	if _, err := conn.CreateProtectedEphemeralSequential(
		path,
		data,
		zk.WorldACL(zk.PermAll),
	); err != nil {
		return nil, err
	}

	return conn, nil
}
