package test

import (
	puddlestore "puddlestore/pkg"
	"testing"
)

func writeFile(client puddlestore.Client, path string, offset uint64, data []byte) error {
	fd, err := client.Open(path, true, true)
	if err != nil {
		return err
	}
	defer client.Close(fd)

	return client.Write(fd, 0, data)
}

func readFile(client puddlestore.Client, path string, offset, size uint64) ([]byte, error) {
	fd, err := client.Open(path, true, false)
	if err != nil {
		return nil, err
	}
	defer client.Close(fd)

	return client.Read(fd, offset, size)
}

// func TestReadWrite(t *testing.T) {
// 	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer cluster.Shutdown()

// 	client, err := cluster.NewClient()
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	in := "test"
// 	if err := writeFile(client, "/a", 0, []byte(in)); err != nil {
// 		t.Fatal(err)
// 	}

// 	var out []byte
// 	if out, err = readFile(client, "/a", 0, 5); err != nil {
// 		t.Fatal(err)
// 	}

// 	if in != string(out) {
// 		t.Fatalf("Expected: %v, Got: %v", in, string(out))
// 	}
// }

func TestClient(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Errorf("%v, %v", client, err)
	}
}

func TestTwoClient(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Errorf("%v, %v", client, err)
	}
	client, err = cluster.NewClient()
	if err != nil {
		t.Errorf("%v, %v", client, err)
	}
}

func TestCreateUnderRoot(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Errorf("%v, %v", client, err)
	}
	fd, err := client.Open("/a.txt", true, false)
	if err != nil || fd < 0 {
		t.Errorf("%v, %v", fd, err)
	}
}

func TestReadNonexistFile(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Errorf("%v, %v", client, err)
	}
	_, err = client.Read(100, 0, 10)
	if err == nil {
		t.Errorf("%v", err)
	}
}
