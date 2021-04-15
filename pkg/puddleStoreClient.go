package pkg

import (
	"fmt"
	"math"

	tapestry "tapestry/pkg"

	"github.com/samuel/go-zookeeper/zk"
)

//may also need local map to provide concurrency

type fileInfo struct {
	Filename string
	Inode    *inode
	Flush    bool
	Modified map[int]string
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
				Filename: path,
				Flush:    write,
				Inode:    &node,
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
	// TODO when path is an dir, it should return err
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
	if len(info.Modified) != 0 {
		for numBlock, guid := range info.Modified {
			path := info.Filename + "-" + string(numBlock)
			data := []byte(guid)
			acl := zk.WorldACL(zk.PermAll)
			_, err := p.Conn.Create(path, data, 0, acl)
			if err != nil {
				//unlock
				return err
			}
		}
	}
	//clear fd
	delete(p.info, fd)
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
	children, _, eventChan, err := p.Conn.ChildrenW("/tapestry")
	if err != nil {
		//unlock
		return []byte{}, fmt.Errorf("Get ChildrenW err")
	}
	//TODO:generate a random path from children
	child := children[0]
	remote, err := p.getRandomRemote(child)
	if err != nil {
		//unlock
		return []byte{}, fmt.Errorf("Unexpected err")
	}
	for ; start <= end; start++ {
		//TODO: if cached, read locally
		select {
		case event := <-eventChan:
			if event.Type == zk.EventNodeCreated {
				children = append(children, event.Path)
			}
			if event.Type == zk.EventNodeDeleted {
				//TODO: remove event.Path
				if event.Path == child {
					//TODO: need a new membership server
				}
			}
		}
		//TODO: otherwise, request the data, only one is sufficient?, how to get the data?
	}
	return nil, nil
}

func (p *puddleStoreClient) Write(fd int, offset uint64, data []byte) error {
	//should return a copy of original data array and the client should be able to read its own write
	//if anything fail, should we clear fd?
	info, ok := p.info[fd]
	if !ok {
		return fmt.Errorf("invalid fd")
	}
	//Writing to An Open File With write=false should return errors
	if !info.Flush {
		//should we unlock?
		return fmt.Errorf("write == false")
	}
	if offset > info.Inode.Size {
		//read and compare byte by byte?
		//if offset is greater than size, pad with 0
	}
	//replace the blocks that the user writes to.
	//salt the GUID and publish it -> GUID_X1, GUID_X2
	//need timeout to make sure at least one is published
	//Tapestry has no delete function, so you have all the different versions of a block in Tapestry
	//but only the most recent version will be included in the iNode, do we need to fetch all the replica?
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
	//TODO
	return nil
}

//is it recursive?
func (p *puddleStoreClient) List(path string) ([]string, error) {
	//TODO
	return nil, nil
}

func (p *puddleStoreClient) Exit() {
	p.Conn.Close()
	p.info = map[int]fileInfo{}
	//cleanup (like all the opened fd) and all subsequent calls should return an error
}

func (p *puddleStoreClient) getRandomRemote(path string) (*tapestry.RemoteNode, error) {
	//load balancing: It's better to have client gets a random node each time when need to interact.
	var remote tapestry.RemoteNode
	id, err := p.getTapID(path)
	if err != nil {
		return &remote, err
	}
	addr, err := p.getTapAddr(path)
	if err != nil {
		return &remote, err
	}
	remote = tapestry.RemoteNode{
		ID:      id,
		Address: addr,
	}
	return &remote, nil
}

func (p *puddleStoreClient) getTapID(path string) (tapestry.ID, error) {
	//TODO: get the ID from path := "/tapestry/node-000" + Tap.tap.ID()
	id, err := tapestry.ParseID(path)
	return id, err
}

func (p *puddleStoreClient) getTapAddr(path string) (string, error) {
	addr, _, err := p.Conn.Get(path)
	return string(addr), err
}
