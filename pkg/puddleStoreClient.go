package pkg

import (
	"fmt"
	"math"

	"github.com/samuel/go-zookeeper/zk"
)

//may also need local map to provide concurrency

type fileInfo struct {
	// Filename   string
	Inode    *inode
	Flush    bool
	Modified []int
}

type puddleStoreClient struct {
	Conn      *zk.Conn
	cache     map[int]map[int][]byte //maintain a map from block num -> bytes or tapestry ID() TODO
	fdCounter int
	info      map[int]fileInfo //fd -> fileinfo
}

func (p *puddleStoreClient) getFd() int {
	// if map FileDescripto does not contain fdCounter, return a copy of fdCounter, and increment fdCounter
	if _, ok := p.info[p.fdCounter]; !ok {
		fd := p.fdCounter
		if fd == math.MaxInt32 {
			p.fdCounter = 0
		} else {
			p.fdCounter = p.fdCounter + 1
		}
		return fd
	}
	// otherwise, increment fdCounter until the map does not contain fdCounter, return its copy and increment it
	for i := p.fdCounter + 1; i < math.MaxInt32; i++ {
		if _, ok := p.info[i]; !ok {
			fd := i
			p.fdCounter = i + 1
			return fd
		}
	}
	for i := 0; i < p.fdCounter; i++ {
		if _, ok := p.info[i]; !ok {
			fd := i
			p.fdCounter = i + 1
			return fd
		}
	}
	return -1
}

func (p *puddleStoreClient) isFileExist(path string) (bool, error) {
	//lock
	rlt, _, err := p.Conn.Exists(path)
	//unlock
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
			acl := zk.WorldACL(zk.PermAll)
			_, err = p.Conn.Create(path, data, 0, acl)
			//Creating file without existing directory should throw an error
			if err != nil {
				//unlock
				return fd, err
			}
			fd = p.getFd()
			p.info[fd] = fileInfo{
				Flush: write,
				Inode: &node,
			}
			return fd, nil
		}
	}
	//lock
	data, _, err := p.Conn.Get(path)
	if err != nil {
		//unlock
		return fd, err
	}
	node, err := decodeInode(data)
	if err != nil {
		//unlock
		return fd, err
	}
	// todo when path is an dir, it should return err
	if node.IsDir {
		return fd, fmt.Errorf("it's a directory")
	}
	fd = p.getFd()
	p.info[fd] = fileInfo{
		Flush: write,
		Inode: node,
	}
	return fd, nil
}

//You should retry a few times, between 3 to 10 times if anything fail

func (p *puddleStoreClient) Close(fd int) error {
	//if the map p.Info does not contains fd, return error
	info, ok := p.info[fd]
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
	//clear fd
	return nil
}

func (p *puddleStoreClient) Read(fd int, offset, size uint64) ([]byte, error) {
	//should return a copy of original data array
	info, ok := p.info[fd]
	if !ok {
		return []byte{}, fmt.Errorf("invalid fd")
	}
	//calculate the blocks we need to read
	start := offset / DefaultConfig().BlockSize
	end := info.Inode.Size / DefaultConfig().BlockSize
	if offset+size < info.Inode.Size {
		end = (offset + size) / DefaultConfig().BlockSize
	}
	for ; start <= end; start++ {
		//if cached, read locally
		//otherwise, request the data, only one is sufficient
	}
	return nil, nil
}

func (p *puddleStoreClient) Write(fd int, offset uint64, data []byte) error {
	//should return a copy of original data array and the client should be able to read its own write
	info, ok := p.info[fd]
	if !ok {
		return fmt.Errorf("invalid fd")
	}
	//Writing to An Open File With write=false should return errors
	if !info.Flush {
		return fmt.Errorf("write == false")
	}
	//if offset is greater than size, pad with 0
	//In copy-on-write, you replace the blocks that the user writes to.
	//fetch all the replica
	//Tapestry has no delete function, so you have all the different versions of a block in Tapestry
	//but only the most recent version will be included in the iNode.
	//cache the write
	return nil
}

func (p *puddleStoreClient) Mkdir(path string) error {
	acl := zk.WorldACL(zk.PermAll)
	//lock
	_, err := p.Conn.Create(path, []byte{}, 0, acl)
	//unlock
	return err
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
	p.info = map[int]fileInfo{}
	//cleanup (like all the opened fd) and all subsequent calls should return an error
}

func (p *puddleStoreClient) getGate() string {
	//load balancing: It's better to have client gets a random node each time when need to interact.
	return ""
}
