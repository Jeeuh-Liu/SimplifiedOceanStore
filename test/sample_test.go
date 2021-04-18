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

	path := "/la1"

	fd, err := client.Open(path, true, true)
	if err != nil {
		t.Errorf("%v", err)
	}
	data, err := client.Read(fd, 0, 5)
	if len(data) != 0 || err != nil {
		t.Errorf("should return empty array and nil")
	}
	client.Close(fd)

	err = client.Remove("/la1")
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

	if err := writeFile(client, "/la2", 0, []byte(in)); err != nil {
		t.Fatal(err)
	}
	var out []byte
	if out, err = readFile(client, "/la2", 0, 48); err != nil {
		t.Fatal(err)
	}
	if in != string(out) {
		t.Fatalf("Expected: %v %v, Got: %v %v", in, len(in), string(out), len(out))
	}

	err = client.Remove("/la2")
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
	err = writeFile(client1, "/la3", 0, in)
	if err != nil {
		t.Fatal(err)
	}
	client1.Remove("/la3")
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
	err = writeFile(client1, "/la4", 0, in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := readFile(client1, "/la4", 0, uint64(len(in)))
	if err != nil {
		t.Fatal(err)
	}
	if string(in) != string(out) {
		client1.Remove("/la4")
		t.Fatalf("Expected: %v %v, Got: %v %v", in, len(in), string(out), len(out))
	}
	client1.Remove("/la4")
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
	err = writeFile(client1, "/la5", 0, in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := readFile(client1, "/la5", 0, uint64(1000))
	if err != nil {
		t.Fatal(err)
	}
	if string(in) != string(out) {
		t.Fatalf("Expected: %v %v, Got: %v %v", in, len(in), string(out), len(out))
	}
	client1.Remove("/la5")
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
	fd, err := client.Open("/la6", true, false)
	if err != nil || fd < 0 {
		t.Errorf("%v, %v", fd, err)
	}
	client.Close(fd)
	err = client.Remove("/la6")
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
	fd, err := client.Open("/la7", true, false)
	if err != nil {
		t.Errorf("%v", err)
	}
	err = client.Close(fd)
	if err != nil {
		t.Errorf("%v", err)
	}
	err = client.Remove("/la7")
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
	fd, err := client.Open("/la8", true, true)
	if err != nil {
		t.Errorf("%v", err)
	}
	err = client.Close(fd)
	if err != nil {
		t.Errorf("%v", err)
	}
	err = client.Remove("/la8")
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
	fd, err := client.Open("/la9", true, false)
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
	err = client.Remove("/la9")
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

	if err := writeFile(client, "/la10", 0, []byte(in)); err != nil {
		t.Fatal(err)
	}
	cluster.Shutdown()
	err = client.Remove("/la10")
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
		if err := writeFile(client, "/la12", uint64(i), []byte(in)); err != nil {
			t.Errorf("%v", err)
		}
		var out []byte
		if out, err = readFile(client, "/la12", uint64(i), 6); err != nil {
			t.Errorf("%v", err)
		}
		if in != string(out) {
			t.Errorf("at %v iter, Expected: %v %v, Got: %v %v", i, []byte(in), len(in), out, len(out))
		}
	}

	err = client.Remove("/la12")
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

	if err := writeFile(client, "/la13", 4, []byte(in)); err != nil {
		t.Fatal(err)
	}
	var out []byte
	if out, err = readFile(client, "/la13", 4, 4); err != nil {
		t.Fatal(err)
	}
	if in != string(out) {
		t.Fatalf("Expected: %v %v, Got: %v %v", in, len(in), string(out), len(out))
	}

	err = client.Remove("/la13")
	if err != nil {
		t.Fatal(err)
	}
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

	client.Mkdir("/la16")
	path := "/la16/a"
	fd, err := client.Open(path, true, true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.List("/")
	if err != nil {
		t.Fatal(err)
	}

	err = client.Remove("/la16/a")
	if err != nil {
		t.Fatal(err)
	}

	err = client.Remove("/la16")
	if err != nil {
		t.Fatal(err)
	}

	client.Close(fd)
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

func TestRemove2(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	defer cluster.Shutdown()
	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	err = client.Remove("/ta100")
	if err == nil {
		t.Errorf("list nonexist path")
	}
}

func TestMkdir2(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	defer cluster.Shutdown()
	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	err = client.Mkdir("/a/")
	if err != nil {
		t.Errorf("should mkdir for /a/ based on checkpoint")
	}
	client.Remove("/a/")
	client.Remove("/a")
}

func TestMkRoot(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	defer cluster.Shutdown()
	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	err = client.Mkdir("/")
	if err == nil {
		t.Errorf("should mk root")
	}
}

func TestReadBeyondEmpty(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	defer cluster.Shutdown()
	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	fd, err := client.Open("/la20", true, true)
	if err != nil {
		t.Fatal(err)
	}
	out, err := client.Read(fd, 100, 100)
	if len(out) != 0 || err != nil {
		t.Errorf("WRONG OUTPUT FOR READ BEYOND EMPTY")
	}
	client.Close(fd)
	client.Remove("/la20")
}

func TestWriteEdges(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	defer cluster.Shutdown()
	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	err = client.Write(100, 100, []byte("string"))
	if err == nil {
		t.Errorf("this fd is invalid")
	}
	fd, err := client.Open("/la21", true, false)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Write(fd, 100, []byte("string"))
	if err == nil {
		t.Errorf("this is readonly file")
	}
	client.Close(fd)
	fd, err = client.Open("/la22", true, false)
	if err != nil {
		t.Fatal(err)
	}
	client.Close(fd)
	client.Remove("/la21")
	client.Remove("/la22")
	client.Exit()
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
	path := "/success"

	fd, err := client.Open(path, true, true)
	if err != nil {
		t.Fatal(err)
	}
	fd2, err := client2.Open(path, false, true)
	if err == nil {
		client.Remove("/success")
		t.Fatal("two files opend")
	}
	client.Remove("/success")
	client.Close(fd)
	client2.Close(fd2)
	client.Remove(path)
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
		writeFile(client, "/la17", 0, []byte(in))
	}(client)
	time.Sleep(time.Second)
	if err := writeFile(client, "/la17", 0, []byte(in)); err != nil {
		client.Remove("/la17")
		t.Fatal(err)
	}
	client.Remove("/la17")
	client.Exit()
}

func TestMkdir3(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()
	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	client.Mkdir("/p3")
	client.Mkdir("/p3/lu")
	client.Mkdir("/p3/tk")
	client.Mkdir("/p3/tk/a3")
	client.Remove("/p3/tk")
	list, _ := client.List("/p3")
	client.Remove("/p3/tk/a3")
	client.Remove("/p3/tk")
	client.Remove("/p3/lu")
	client.Remove("/p3")
	t.Errorf("%v", list)
}
