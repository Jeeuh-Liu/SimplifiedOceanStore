package pkg

import (
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"strings"
	"time"

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
	children  []string
}

func (p *puddleStoreClient) init() {
	p.cache = make(map[int]map[int][]byte)
	p.fdCounter = 0
	p.info = make(map[int]fileInfo)
	p.children = make([]string, DefaultConfig().NumTapestry)
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
func (p *puddleStoreClient) lock() {
	for {
		_, err := CreateEphSeq(p.Conn, "/lockhhh", []byte{})
		if err == nil {
			return
		}
		exist, _, eventChan, err := p.Conn.ExistsW("/lockhhh")
		if err != nil {
			continue
		}
		if exist {
			<-eventChan
		}
	}
}

func (p *puddleStoreClient) unlock() {
	_, state, err := p.Conn.Get("/lockhhh")
	if err == nil {
		p.Conn.Delete("/lockhhh", state.Version)
	}
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
	if strings.Compare(path, "/") == 0 {
		return fd, fmt.Errorf("should not open Root")
	}
	exist, err := p.isFileExist(path)
	if err != nil {
		return fd, err
	}
	if !exist {
		if !create {
			return fd, fmt.Errorf("create == false && exist == false, err")
		} else {
			if underFile, err := p.underFile(path); underFile || err != nil {
				return fd, fmt.Errorf("create under file")
			}
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
	// //lock
	// data, _, err := p.Conn.Get(path)
	// if err != nil {
	// 	//unlock
	// 	return fd, err
	// }
	// node, err := decodeInode(data)
	// if err != nil {
	// 	//unlock
	// 	return fd, err
	// }
	// if node.IsDir {
	// 	return fd, fmt.Errorf("it's a directory")
	// }
	// fd = p.getFd()
	// p.info[fd] = fileInfo{
	// 	Flush: write,
	// 	Inode: node,
	// }
	return fd, nil
}

//You should retry a few times, between 3 to 10 times if anything fail
func (p *puddleStoreClient) underFile(path string) (bool, error) {
	dir := filepath.Dir(path)
	data, _, err := p.Conn.Get(dir)
	if err != nil {
		return false, err
	}
	node, err := decodeInode(data)
	if err != nil {
		return false, err
	}
	if node.IsDir {
		return false, nil
	} else {
		return true, nil
	}

}
func (p *puddleStoreClient) Close(fd int) error {
	//if the map p.Info does not contains fd, return error
	// info, ok := p.info[fd]
	_, ok := p.info[fd]
	if !ok {
		return fmt.Errorf("invalid fd")
	}
	//if flush is not needed unlock and return nil
	// if !info.Flush {
	// 	//unlock
	// 	return nil
	// }
	// //update metadata in zookeeper, then unlcok
	// if len(info.Modified) != 0 {
	// 	path := info.Filename
	// 	//it should be inode here
	// 	data, err := encodeInode(*info.Inode)
	// 	if err != nil {
	// 		//unlock
	// 		return fmt.Errorf("err in enode inode")
	// 	}
	// 	_, state, err := p.Conn.Exists(path)
	// 	if err != nil {
	// 		//unlcok
	// 		return fmt.Errorf("unexpected err in zookeeper Exist")
	// 	}
	// 	_, err = p.Conn.Set(path, data, state.Version)
	// 	if err != nil {
	// 		//unlock
	// 		return err
	// 	}
	// }
	// //clear fd
	// delete(p.info, fd)
	return nil
}

func (p *puddleStoreClient) Read(fd int, offset, size uint64) ([]byte, error) {
	//should return a copy of original data array
	// info, ok := p.info[fd]
	_, ok := p.info[fd]
	if !ok {
		return []byte{}, fmt.Errorf("invalid fd")
	}
	//calculate the blocks we need to read
	// startBlock := offset / DefaultConfig().BlockSize
	// endBlock := info.Inode.Size / DefaultConfig().BlockSize
	// if offset+size < info.Inode.Size {
	// 	endBlock = (offset + size) / DefaultConfig().BlockSize
	// }
	// offset = offset % DefaultConfig().BlockSize
	// var rlt []byte
	// var bytesRead uint64
	// bytesRead = 0
	// for i := startBlock; i <= endBlock; i++ {
	// 	//if cached, read locally
	// 	var data []byte
	// 	if data, ok := p.cache[fd][int(startBlock)]; ok {
	// 		rlt = append(rlt, data...)
	// 	} else {
	// 		remote, err := p.connectRemote()
	// 		if err != nil {
	// 			//unlock
	// 			return []byte{}, err
	// 		}
	// 		data, err = remote.Get(info.Filename)
	// 		if err != nil {
	// 			//unlock
	// 			return []byte{}, err
	// 		}
	// 	}
	// 	if i == startBlock {
	// 		data = data[offset:]
	// 	} else {
	// 		if i == endBlock {
	// 			left := size - bytesRead
	// 			if left < DefaultConfig().BlockSize {
	// 				data = data[:left]
	// 			}
	// 		}
	// 	}
	// 	bytesRead = bytesRead + uint64(len(data))
	// 	rlt = append(rlt, data...)
	// }
	// return rlt, nil
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
	// if offset > info.Inode.Size, [info.Inode.Size, offset) should be filled with 0
	// write data []byte
	// for each block, salt DefaultConfig().NumReplicas times and publish it
	// make sure at least one is published
	// cache the write
	// startBlock := offset / DefaultConfig().BlockSize
	// endBlock := (offset + uint64(len(data))) / DefaultConfig().BlockSize
	// currentBlock := info.Inode.Size / DefaultConfig().BlockSize
	// if offset > info.Inode.Size {
	// 	p.readBlock(info.Filename, fd, int(currentBlock))
	// 	for startBlock > currentBlock {
	// 		currentBlock += 1
	// 		p.publish(info.Filename, make([]byte, DefaultConfig().BlockSize))
	// 	}
	// 	p.publish(info.Filename, make([]byte, offset%DefaultConfig().BlockSize))
	// 	// if offset > info.Inode.Size, [info.Inode.Size, offset) should be filled with 0

	// }
	// if startBlock == endBlock {
	// 	tmp, err := p.readBlock(info.Filename, fd, int(startBlock))
	// 	if err != nil {
	// 		return err
	// 	}
	// 	r := tmp[:offset%DefaultConfig().BlockSize]
	// 	r = append(r, data...)
	// 	r = append(r, tmp[(offset+uint64(len(data)))%DefaultConfig().BlockSize:]...)
	// 	p.publish(info.Filename, r)
	// 	p.cache[fd][int(startBlock)] = r
	// } else {
	// 	tmp, err := p.readBlock(info.Filename, fd, int(startBlock))
	// 	if err != nil {
	// 		return err
	// 	}
	// 	r := tmp[:offset%DefaultConfig().BlockSize]
	// 	initbytes := DefaultConfig().BlockSize - offset%DefaultConfig().BlockSize
	// 	r = append(r, data[:initbytes]...)
	// 	p.publish(info.Filename, r)
	// 	p.cache[fd][int(startBlock)] = r
	// 	for i := startBlock + 1; i <= endBlock-1; i++ {
	// 		r = data[initbytes+(i-startBlock-1)*DefaultConfig().BlockSize : initbytes+(i-startBlock)*DefaultConfig().BlockSize]
	// 		p.publish(info.Filename, r)
	// 		p.cache[fd][int(i)] = r
	// 	}
	// 	tmp, err = p.readBlock(info.Filename, fd, int(endBlock))
	// 	if err != nil {
	// 		return err
	// 	}
	// 	r = data[initbytes+(endBlock-startBlock-1)*DefaultConfig().BlockSize:]
	// 	r = append(r, tmp[(offset+uint64(len(data)))%DefaultConfig().BlockSize:]...)
	// 	p.publish(info.Filename, r)
	// 	p.cache[fd][int(endBlock)] = r
	// }
	return nil
}

func (p *puddleStoreClient) readBlock(filename string, fd, numBlock int) ([]byte, error) {
	if data, ok := p.cache[fd][numBlock]; ok {
		return data, nil
	} else {
		remote, err := p.connectRemote()
		if err != nil {
			return []byte{}, err
		}
		data, err = remote.Get(filename)
		if err != nil {
			return []byte{}, err
		}
		return data, nil
	}
}

func (p *puddleStoreClient) publish(filename string, data []byte) error {
	remotes, err := p.connectRemotes()
	if err != nil {
		return err
	}
	success := false
	for i, remote := range remotes {
		err = remote.Store(filename+string(i), data)
		if err != nil {
			continue
		} else {
			success = true
		}
	}
	if !success {
		return fmt.Errorf("none of publish success")
	} else {
		return nil
	}
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
	// exist, err := p.isFileExist(path)
	// if err != nil {
	// 	return err
	// }
	// if !exist {
	// 	return fmt.Errorf("not exist")
	// }
	// //lock
	// data, state, err := p.Conn.Get(path)
	// if err != nil {
	// 	//unlock
	// 	return err
	// }
	// node, err := decodeInode(data)
	// if err != nil {
	// 	//unlock
	// 	return err
	// }
	// //unlock
	// if node.IsDir {
	// 	//locked?
	// 	children, _, err := p.Conn.Children(path)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	for _, s := range children {
	// 		p.Remove(s)
	// 	}
	// 	p.Conn.Delete(path, state.Version) //version
	// } else {
	// 	//locked?
	// 	//lock
	// 	p.Conn.Delete(path, state.Version)
	// 	//unlock
	// }

	return nil
}

func (p *puddleStoreClient) List(path string) ([]string, error) {
	exist, err := p.isFileExist(path)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, fmt.Errorf("not exist")
	}
	children, _, err := p.Conn.Children(path)
	if err != nil {
		return nil, err
	}
	return children, nil
}
func (p *puddleStoreClient) Exit() {
	p.Conn.Close()
	p.info = map[int]fileInfo{}
	//cleanup (like all the opened fd) and all subsequent calls should return an error
}

func (p *puddleStoreClient) connectRemote() (*tapestry.Client, error) {
	//load balancing: It's better to have client gets a random node each time when need to interact.
	rand.Seed(time.Now().UnixNano())
	path := p.children[rand.Intn(len(p.children))]
	addr, _, err := p.Conn.Get(path)
	if err != nil {
		return nil, err
	}
	remote, err := tapestry.Connect(string(addr))
	return remote, err
}

func (p *puddleStoreClient) connectRemotes() ([]*tapestry.Client, error) {
	//load balancing: It's better to have client gets a random node each time when need to interact.
	//choose  random remote paths from p.children
	var remotes []*tapestry.Client
	p.shuffleChildren()
	for i := 0; i < len(p.children) && len(remotes) < DefaultConfig().NumReplicas; i++ {
		path := p.children[i]
		addr, _, err := p.Conn.Get(path)
		if err != nil {
			return nil, err
		}
		remote, err := tapestry.Connect(string(addr))
		if err == nil {
			remotes = append(remotes, remote)
		}
	}
	// if len(remotes) < DefaultConfig().NumReplicas
	return remotes, nil
}

func (p *puddleStoreClient) watch() {
	children, _, eventChan, _ := p.Conn.ChildrenW("/tapestry")
	p.children = children
	for {
		event := <-eventChan
		if event.Type == zk.EventNodeCreated {
			p.children = append(p.children, event.Path)
		}
		if event.Type == zk.EventNodeDeleted {
			len := len(p.children)
			for i := range p.children {
				if p.children[i] != event.Path {
					continue
				}
				p.children[len-1], p.children[i] = p.children[i], p.children[len-1]
				p.children = p.children[:len-1]
			}
		}
	}
}

func (p *puddleStoreClient) shuffleChildren() {
	rand.Seed(time.Now().UnixNano())
	for i := len(p.children) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		p.children[i], p.children[j] = p.children[j], p.children[i]
	}
}

func (p *puddleStoreClient) connectAll() {
	children, _, _, _ := p.Conn.ChildrenW("/tapestry")
	for i := 0; i < len(children); i++ {
		port, _, _ := p.Conn.Get(filepath.Join("/tapestry", children[i]))
		tapestry.Connect(string(port))
	}
}
