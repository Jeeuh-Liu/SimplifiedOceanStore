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

	return client.Write(fd, offset, data)
}

func readFile(client puddlestore.Client, path string, offset, size uint64) ([]byte, error) {
	fd, err := client.Open(path, true, false)
	if err != nil {
		return nil, err
	}
	defer client.Close(fd)

	return client.Read(fd, offset, size)
}

// func writeFile2(client puddlestore.Client, path string, offset uint64, data []byte) error {
// 	fd, err := client.Open(path, true, true)
// 	if err != nil {
// 		return err
// 	}
// 	defer client.Close(fd)

// 	return client.Write2(fd, offset, data)
// }

// func readFile2(client puddlestore.Client, path string, offset, size uint64) ([]byte, error) {
// 	fd, err := client.Open(path, true, false)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer client.Close(fd)

// 	return client.Read2(fd, offset, size)
// }

func TestReadEmptyFile(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	path := "/a"

	fd, err := client.Open(path, true, true)
	if err != nil {
		t.Errorf("%v", err)
	}
	data, err := client.Read(fd, 0, 5)
	if len(data) != 0 || err != nil {
		t.Errorf("should return empty array and nil")
	}
}
func TestReadWrite(t *testing.T) {
	config := puddlestore.Config{
		BlockSize:   64,
		NumReplicas: 2,
		NumTapestry: 2,
		ZkAddr:      "localhost:2181", // restore to localhost:2181 before submitting
	}
	cluster, err := puddlestore.CreateCluster(config)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	in := "testtesttesttesttesttesttesttesttesttesttesttest"

	if err := writeFile(client, "/a", 0, []byte(in)); err != nil {
		t.Fatal(err)
	}
	var out []byte
	if out, err = readFile(client, "/a", 0, 48); err != nil {
		t.Fatal(err)
	}
	if in != string(out) {
		t.Fatalf("Expected: %v %v, Got: %v %v", in, len(in), string(out), len(out))
	}
}

func TestWriteEmptyWithOffset(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	in := "muddle"

	// for i := 0; i < 58; i++ {
	// 	if err := writeFile(client, "/a", uint64(i), []byte(in)); err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	var out []byte
	// 	if out, err = readFile(client, "/a", uint64(i), uint64(i+6)); err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	if in != string(out) {
	// 		t.Fatalf("at %v iter, Expected: %v %v, Got: %v %v", i, []byte(in), len(in), out, len(out))
	// 	}
	// }
	i := 58
	if err = writeFile(client, "/a", uint64(i), []byte(in)); err != nil {
		t.Fatal(err)
	}
	out, err := readFile(client, "/a", uint64(i), uint64(i+6))
	if err != nil {
		t.Fatalf("%v %v", []byte(in), out)
	}
	if in != string(out) {
		t.Fatalf("at 1 iter, Expected: %v %v, Got: %v %v", []byte(in), len(in), out, len(out))
	}

}
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

func TestNormalClose1(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Errorf("%v, %v", client, err)
	}
	fd, err := client.Open("/b.txt", true, false)
	if err != nil {
		t.Errorf("%v", err)
	}
	err = client.Close(fd)
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestNormalClose2(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Errorf("%v, %v", client, err)
	}
	fd, err := client.Open("/b.txt", true, true)
	if err != nil {
		t.Errorf("%v", err)
	}
	err = client.Close(fd)
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestReadClosedFile(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Errorf("%v, %v", client, err)
	}
	fd, err := client.Open("/b.txt", true, false)
	if err != nil {
		t.Errorf("%v", err)
	}
	err = client.Close(fd)
	if err != nil {
		t.Errorf("%v", err)
	}
	_, err = client.Read(fd, 0, 10)
	if err == nil {
		t.Errorf("the fd is invalid")
	}
}
