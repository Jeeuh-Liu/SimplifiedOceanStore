package test

import (
	puddlestore "puddlestore/pkg"
	"testing"
	"time"
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
	client.Close(fd)

	err = client.Remove("/a")
	if err != nil {
		t.Fatal(err)
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

	err = client.Remove("/a")
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteAcrossBlock(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()
	in := make([]byte, 200)
	for i := range in {
		in[i] = 1
	}
	client1, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	err = writeFile(client1, "/a", 0, in)
	if err != nil {
		t.Fatal(err)
	}
	client1.Remove("/a")
	client1.Exit()
}

func TestReadAcrossBlock(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()
	in := make([]byte, 200)
	for i := range in {
		in[i] = 1
	}
	client1, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	err = writeFile(client1, "/a", 0, in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := readFile(client1, "/a", 0, uint64(len(in)))
	if err != nil {
		t.Fatal(err)
	}
	if string(in) != string(out) {
		client1.Remove("/a")
		t.Fatalf("Expected: %v %v, Got: %v %v", in, len(in), string(out), len(out))
	}
	client1.Remove("/a")
	client1.Exit()
}

func TestReadBeyondFileSize(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()
	in := make([]byte, 100)
	for i := range in {
		in[i] = 1
	}
	client1, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	err = writeFile(client1, "/a", 0, in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := readFile(client1, "/a", 0, uint64(1000))
	if err != nil {
		t.Fatal(err)
	}
	if string(in) != string(out) {
		t.Fatalf("Expected: %v %v, Got: %v %v", in, len(in), string(out), len(out))
	}
	client1.Remove("/a")
	client1.Exit()
}

func TestClient(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	client, err := cluster.NewClient()
	if err != nil {
		t.Errorf("%v, %v", client, err)
	}

	cluster.Shutdown()
}

func TestTwoClient(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	client, err := cluster.NewClient()
	if err != nil {
		t.Errorf("%v, %v", client, err)
	}
	client, err = cluster.NewClient()
	if err != nil {
		t.Errorf("%v, %v", client, err)
	}

	cluster.Shutdown()
}

func TestCreateUnderRoot(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	client, err := cluster.NewClient()
	if err != nil {
		t.Errorf("%v, %v", client, err)
	}
	fd, err := client.Open("/a.txt", true, false)
	if err != nil || fd < 0 {
		t.Errorf("%v, %v", fd, err)
	}
	client.Close(fd)
	err = client.Remove("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	cluster.Shutdown()
}

func TestReadNonexistFile(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	client, err := cluster.NewClient()
	if err != nil {
		t.Errorf("%v, %v", client, err)
	}
	_, err = client.Read(100, 0, 10)
	if err == nil {
		t.Errorf("%v", err)
	}

	cluster.Shutdown()

}

func TestNormalClose1(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

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
	err = client.Remove("/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	cluster.Shutdown()
}

func TestNormalClose2(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

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
	err = client.Remove("/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	cluster.Shutdown()
}

func TestReadClosedFile(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
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
	err = client.Remove("/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	cluster.Shutdown()
}

func TestRemove(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	in := "muddle"

	if err := writeFile(client, "/a", 0, []byte(in)); err != nil {
		t.Fatal(err)
	}
	cluster.Shutdown()
	err = client.Remove("/a")
	if err != nil {
		t.Fatal(err)
	}
}

func TestWatch(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	cluster.Shutdown()
	time.Sleep(3 * time.Second)
	client.Exit()
}

func TestCreateRoot(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Open("/", true, true)
	if err == nil {
		t.Fatal(err)
	}
}

func TestOpenNonExistWithoutCreate(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Open("/lalalaa", false, true)
	if err == nil {
		t.Fatal(err)
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

	for i := 58; i < 63; i++ {
		if err := writeFile(client, "/a", uint64(i), []byte(in)); err != nil {
			t.Errorf("%v", err)
		}
		var out []byte
		if out, err = readFile(client, "/a", uint64(i), 6); err != nil {
			t.Errorf("%v", err)
		}
		if in != string(out) {
			t.Errorf("at %v iter, Expected: %v %v, Got: %v %v", i, []byte(in), len(in), out, len(out))
		}
	}

	err = client.Remove("/a")
	if err != nil {
		t.Fatal(err)
	}
}

// func TestCreateUnderFile(t *testing.T) {
// 	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	client, err := cluster.NewClient()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	in := make([]byte, 100)
// 	for i := range in {
// 		in[i] = 1
// 	}
// 	err = writeFile(client, "/a", 0, in)
// 	if err == nil {
// 		t.Fatal(err)
// 	}
// 	_, err = client.Open("/a/a.txt", true, true)
// 	if err == nil {
// 		client.Remove("/a")
// 		client.Remove("/a/a.txt")
// 		t.Fatal(err)
// 	}
// 	client.Remove("/a")
// }
func TestReadWrite2(t *testing.T) {
	config := puddlestore.Config{
		BlockSize:   4,
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

	in := "test"

	if err := writeFile(client, "/a", 4, []byte(in)); err != nil {
		t.Fatal(err)
	}
	var out []byte
	if out, err = readFile(client, "/a", 4, 4); err != nil {
		t.Fatal(err)
	}
	if in != string(out) {
		t.Fatalf("Expected: %v %v, Got: %v %v", in, len(in), string(out), len(out))
	}

	err = client.Remove("/a")
	if err != nil {
		t.Fatal(err)
	}
}

func TestOpenSameFile(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()
	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	client2, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	path := "/a"

	fd, err := client.Open(path, true, true)
	if err != nil {
		t.Fatal(err)
	}
	fd2, err := client2.Open(path, true, true)
	if err == nil {
		t.Fatal("two files opend")
	}
	client.Close(fd)
	client2.Close(fd2)
	client.Remove(path)
}

func TestList(t *testing.T) {

	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	defer cluster.Shutdown()
	client, err := cluster.NewClient()

	if err != nil {
		t.Fatal(err)
	}

	client.Mkdir("/b")
	path := "/b/a"
	fd, err := client.Open(path, true, true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.List("/")
	if err != nil {
		t.Fatal(err)
	}

	err = client.Remove("/b")
	if err != nil {
		t.Fatal(err)
	}

	client.Close(fd)
}

func TestConWrite(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	defer cluster.Shutdown()
	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	in := "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest"

	go func(client puddlestore.Client) {
		in := "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest"
		writeFile(client, "/a", 0, []byte(in))
	}(client)
	time.Sleep(time.Second)
	if err := writeFile(client, "/a", 0, []byte(in)); err != nil {
		client.Remove("/a")
		t.Fatal(err)
	}
	client.Remove("/a")
	client.Exit()
}

func TestList2(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	defer cluster.Shutdown()
	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.List("ta")
	if err == nil {
		t.Errorf("list nonexist path")
	}
	_, err = client.List("/ta")
	if err == nil {
		t.Errorf("list nonexist path")
	}
}
