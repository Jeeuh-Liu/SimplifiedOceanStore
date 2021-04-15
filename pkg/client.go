package pkg

import "fmt"

// Client is a puddlestore client interface that will communicate with puddlestore nodes
type Client interface {
	// `Open` opens a file and returns a file descriptor. If the `create` is true and the
	// file does not exist, create the file. If `create` is false and the file does not exist,
	// return an error. If `write` is true, then flush the resulting inode on Close(). If `write`
	// is false, no need to flush the inode to zookeeper. If `Open` is successful, the returned
	// file descriptor should be unique to the file. The client is responsible for keeping
	// track of local file descriptors. Using `Open` allows for file-locking and
	// multi-operation transactions.
	Open(path string, create, write bool) (int, error)

	// `Close` closes the file and flushes its contents to the distributed filesystem.
	// The closed fd should be available again after successfully closing. We only flush
	// changes to the file on close to ensure copy-on-write atomicity of operations. Refer
	// to the handout for more information on why this is necessary.
	Close(fd int) error

	// `Read` returns a `size` amount of bytes starting at `offset` in an opened file.
	// Reading at non-existent offset returns empty buffer and no error.
	// If offset+size exceeds file boundary, return as much as possible with no error.
	// Returns err if fd is not opened.
	Read(fd int, offset, size uint64) ([]byte, error)

	// `Write` writes `data` starting at `offset` on an opened file. Writing beyond the
	// file boundary automatically fills the file with zero bytes. Returns err if fd is not opened.
	Write(fd int, offset uint64, data []byte) error

	// `Mkdir` creates directory at the specified path.
	// Returns error if any parent directory does not exist (non-recursive).
	Mkdir(path string) error

	// `Remove` removes a directory or file. Returns err if not exists.
	Remove(path string) error

	// `List` lists file & directory names (not full names) under `path`. Returns err if not exists.
	List(path string) ([]string, error)

	// Release zk connection. Students don't have this and may add one
	Exit()
}

type fileInfo struct {
	// Filename   string
	ReadCache  map[int][]byte //maintain a map from block num -> bytes or tapestry ID() TODO
	WriteCache map[int][]byte //maintain a map from block num -> bytes or tapestry ID() TODO
	Flush      bool
}
type puddleStoreClient struct {
	Tap       *Tapestry
	Info      map[int]fileInfo
	fdCounter int
}

//Constructor

func (p *puddleStoreClient) getFd() int {
	// if map FileDescripto does not contain fdCounter, return a copy of fdCounter, and increment fdCounter
	// otherwise, increment fdCounter until the map does not contain fdCounter, return its copy and increment it
	// round if necessary

}

func (p *puddleStoreClient) isFileExist(path string) (bool, error) {
	rlt, _, err := p.Tap.zk.Exists(path)
	if err != nil {
		return rlt, err
	} else {
		return rlt, nil
	}
}

func (p *puddleStoreClient) Open(path string, create, write bool) (int, error) {
	//lock
	//todo when path is an dir, it should return err
	fd := -1
	exist, err := p.isFileExist(path)
	if err != nil {
		return fd, err
	}
	//TODO: Creating file without existing directory should throw an error
	if !exist {
		if !create {
			return fd, fmt.Errorf("create == false && exist == false, err!")
		} else {
			data, err := encodeInode(newInode())
			if err != nil {
				return fd, err
			}
			//create an ephemeral znode??? set corresponding flags and acl
			//it will return a path in zookeeper, will we use it?
			_, err = p.Tap.zk.Create(path, data, flags, acl)
			if err != nil {
				return fd, err
			}
		}
	}
	fd = p.getFd()
	//I guess open means prepare fileInfo based on write: map[fd] = newFileInfo()
}

//load balancing: It's better to have client gets a random node each time when need to interact.
//You should retry a few times, between 3 to 10 times if anything fail
//read own write

func (p *puddleStoreClient) Close(fd int) error {
	//if the map p.Info does not contains fd, return error
	//if flush is not needed unlock and return nil
	//update metadata in zookeeper
	//unlock
	//salt the GUID and publish it
	//need timeout to make sure at least one is published
}

func (p *puddleStoreClient) Read(fd int, offset, size uint64) ([]byte, error) {
	//should return a copy of original array
	//try read cache first
	//Blocks that are read from tapestry will be cached locally.
}

func (p *puddleStoreClient) Write(fd int, offset uint64, data []byte) error {
	//should return a copy of original array
	//if offset is greater than size, pad with 0
	//Writing to An Open File With write=false should return errors

	//In copy-on-write, you replace the blocks that the user writes to.
	//Tapestry has no delete function, so you have all the different versions of a block in Tapestry
	//but only the most recent version will be included in the iNode.
}

func (p *puddleStoreClient) Mkdir(path string) error {

}

func (p *puddleStoreClient) Remove(path string) error {
	//lock is not required for Remove
	//if it is a dir, it should recursively remove its descendents
}

func (p *puddleStoreClient) List(path string) ([]string, error) {

}

func (p *puddleStoreClient) Exit() {
	p.Tap.zk.Close()
	//cleanup (like all the opened fd) and all subsequent calls should return an error
}
