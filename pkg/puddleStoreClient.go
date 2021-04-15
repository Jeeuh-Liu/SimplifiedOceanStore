package pkg

import (
	"fmt"
	"math"

	"github.com/samuel/go-zookeeper/zk"
)

type fileInfo struct {
	// Filename   string
	ReadCache  map[int][]byte //maintain a map from block num -> bytes or tapestry ID() TODO
	WriteCache map[int][]byte //maintain a map from block num -> bytes or tapestry ID() TODO
	Flush      bool
}

type puddleStoreClient struct {
	Conn      *zk.Conn
	Info      map[int]fileInfo //fd -> fileinfo
	FdCounter int
}

func (p *puddleStoreClient) getFd() int {
	// if map FileDescripto does not contain fdCounter, return a copy of fdCounter, and increment fdCounter
	if _, ok := p.Info[p.FdCounter]; !ok {
		fd := p.FdCounter
		if fd == math.MaxInt32 {
			p.FdCounter = 0
		} else {
			p.FdCounter = p.FdCounter + 1
		}
		return fd
	}
	// otherwise, increment fdCounter until the map does not contain fdCounter, return its copy and increment it
	for i := p.FdCounter + 1; i < math.MaxInt32; i++ {
		if _, ok := p.Info[i]; !ok {
			fd := i
			p.FdCounter = i + 1
			return fd
		}
	}
	for i := 0; i < p.FdCounter; i++ {
		if _, ok := p.Info[i]; !ok {
			fd := i
			p.FdCounter = i + 1
			return fd
		}
	}
	return -1
}

func (p *puddleStoreClient) isFileExist(path string) (bool, error) {
	rlt, _, err := p.Conn.Exists(path)
	if err != nil {
		return rlt, err
	} else {
		return rlt, nil
	}
}

func (p *puddleStoreClient) Open(path string, create, write bool) (int, error) {
	fd := -1
	exist, err := p.isFileExist(path)
	if err != nil {
		return fd, err
	}
	if !exist {
		if !create {
			return fd, fmt.Errorf("create == false && exist == false, err")
		} else {
			// todo when path is an dir, it should return err
			//TODO: Creating file without existing directory should throw an error
			node := inode{
				IsDir:    false,
				Filename: path,
			}
			data, err := encodeInode(node)
			if err != nil {
				return fd, err
			}
			//may change the flag
			//lock
			_, err = p.Conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				//unlock
				return fd, err
			}
		}
	}
	fd = p.getFd()
	p.Info[fd] = fileInfo{
		Flush: write,
	}
	return 0, nil
}

//load balancing: It's better to have client gets a random node each time when need to interact.
//You should retry a few times, between 3 to 10 times if anything fail
//read own write

func (p *puddleStoreClient) Close(fd int) error {
	//if the map p.Info does not contains fd, return error
	info, ok := p.Info[fd]
	if !ok {
		return fmt.Errorf("invalid fd")
	}
	//if flush is not needed unlock and return nil
	if !info.Flush {
		//unlock
		return nil
	}
	//update metadata in zookeeper, then unlcok
	//salt the GUID and publish it -> GUID_X1, GUID_X2
	//need timeout to make sure at least one is published
	return nil
}

func (p *puddleStoreClient) Read(fd int, offset, size uint64) ([]byte, error) {
	//should return a copy of original array
	//try read cache first
	info, ok := p.Info[fd]
	if !ok {
		return fmt.Errorf("invalid fd")
	}
	// data, ok := info.ReadCache[]
	//Blocks that are read from tapestry will be cached locally.

	//
	return nil, nil
}

func (p *puddleStoreClient) Write(fd int, offset uint64, data []byte) error {
	//should return a copy of original array
	//if offset is greater than size, pad with 0
	//Writing to An Open File With write=false should return errors

	//In copy-on-write, you replace the blocks that the user writes to.
	//Tapestry has no delete function, so you have all the different versions of a block in Tapestry
	//but only the most recent version will be included in the iNode.
	return nil
}

func (p *puddleStoreClient) Mkdir(path string) error {
	return nil
}

func (p *puddleStoreClient) Remove(path string) error {
	//lock is not required for Remove
	//if it is a dir, it should recursively remove its descendents
	return nil
}

func (p *puddleStoreClient) List(path string) ([]string, error) {
	return nil, nil
}

func (p *puddleStoreClient) Exit() {
	p.Conn.Close()
	//cleanup (like all the opened fd) and all subsequent calls should return an error
}
