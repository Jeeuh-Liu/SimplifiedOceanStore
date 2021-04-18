package pkg

import (
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	tapestry "tapestry/pkg"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

//may also need local map to provide concurrency
const RETRY = 10

type fileInfo struct {
	Inode    *inode
	Flush    bool
	Modified map[int]bool
}

type puddleStoreClient struct {
	Conn      *zk.Conn
	ClientMtx sync.Mutex                // Mutex for concurrent access to Client
	cache     map[string]map[int][]byte //maintain a map from block num -> bytes or tapestry ID() TODO
	fdCounter int
	info      map[int]fileInfo //fd -> fileinfo
	children  []string
	config    Config
	seq       int
}

func (p *puddleStoreClient) init(config Config) {
	p.cache = make(map[string]map[int][]byte)
	p.fdCounter = 0
	p.info = make(map[int]fileInfo)
	p.children = make([]string, 0)
	p.config = config
}

func (p *puddleStoreClient) getFd() int {
	// if map FileDescripto does not contain fdCounter, return a copy of fdCounter, and increment fdCounter
	p.ClientMtx.Lock()
	if _, ok := p.info[p.fdCounter]; !ok {
		fd := p.fdCounter
		if fd == math.MaxInt32 {
			p.fdCounter = 0
		} else {
			p.fdCounter = p.fdCounter + 1
		}
		p.ClientMtx.Unlock()
		return fd
	}
	// otherwise, increment fdCounter until the map does not contain fdCounter, return its copy and increment it
	// for i := p.fdCounter + 1; i < math.MaxInt32; i++ {
	// 	if _, ok := p.info[i]; !ok {
	// 		fd := i
	// 		p.fdCounter = i + 1
	// 		p.ClientMtx.Unlock()
	// 		return fd
	// 	}
	// }
	// for i := 0; i < p.fdCounter; i++ {
	// 	if _, ok := p.info[i]; !ok {
	// 		fd := i
	// 		p.fdCounter = i + 1
	// 		p.ClientMtx.Unlock()
	// 		return fd
	// 	}
	// }
	p.ClientMtx.Unlock()
	return -1
}

func (p *puddleStoreClient) getSeq() int {
	// if map FileDescripto does not contain fdCounter, return a copy of fdCounter, and increment fdCounter
	seq := p.seq
	if seq == math.MaxInt32 {
		p.seq = 0
	} else {
		p.seq = p.seq + 1
	}
	return seq
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
	p.lock()
	rlt, _, err := p.Conn.Exists(path)
	p.unlock()
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
			underFile, err := p.underFile(path)
			if err != nil {
				return fd, err
			}
			if underFile {
				return fd, fmt.Errorf("create under file")
			}
			node := inode{
				IsDir:    false,
				Filename: path,
				Blocks:   make(map[int][]string),
				Size:     0,
			}
			data, err := encodeInode(node)
			if err != nil {
				return fd, err
			}
			//may change the flag
			p.lock()
			acl := zk.WorldACL(zk.PermAll)
			_, err = p.Conn.Create(path, data, 0, acl)
			//Creating file without existing directory should throw an error
			if err != nil {
				p.unlock()
				return fd, err
			}
			fd = p.getFd()
			p.ClientMtx.Lock()
			p.info[fd] = fileInfo{
				Flush:    write,
				Inode:    &node,
				Modified: make(map[int]bool),
			}
			p.ClientMtx.Unlock()
			return fd, nil
		}
	}
	p.lock()
	data, _, err := p.Conn.Get(path)
	if err != nil {
		p.unlock()
		return fd, err
	}
	node, err := decodeInode(data)
	if err != nil {
		p.unlock()
		return fd, err
	}
	if node.IsDir {
		return fd, fmt.Errorf("it's a directory")
	}
	fd = p.getFd()
	p.ClientMtx.Lock()
	p.info[fd] = fileInfo{
		Flush:    write,
		Inode:    node,
		Modified: make(map[int]bool),
	}
	p.ClientMtx.Unlock()
	return fd, nil
}

//You should retry a few times, between 3 to 10 times if anything fail
func (p *puddleStoreClient) underFile(path string) (bool, error) {
	dir := filepath.Dir(path)
	if len(dir) == 1 && dir[0] == 47 {
		return false, nil
	}
	p.lock()
	data, _, err := p.Conn.Get(dir)
	p.unlock()
	if err != nil {
		return false, fmt.Errorf("zk err, %v", err)
	}
	node, err := decodeInode(data)
	if err != nil {
		return false, fmt.Errorf("decode err, %v", err)
	}
	if node.IsDir {
		return false, nil
	} else {
		return true, nil
	}

}

func (p *puddleStoreClient) Close(fd int) error {
	//if the map p.Info does not contains fd, return error
	p.ClientMtx.Lock()
	info, ok := p.info[fd]
	p.ClientMtx.Unlock()
	if !ok {
		return fmt.Errorf("invalid fd")
	}
	//if flush is not needed unlock and return nil
	if !info.Flush {
		p.unlock()
		p.ClientMtx.Lock()
		delete(p.info, fd)
		p.ClientMtx.Unlock()
		return nil
	}
	// //update metadata in zookeeper, then unlcok
	if len(info.Modified) != 0 {
		err := p.publish(fd)
		if err != nil {
			for i := 0; i < RETRY; i++ {
				err = p.publish(fd)
				if err == nil {
					break
				}
			}
			if err != nil {
				p.unlock()
				return fmt.Errorf("publish fail")
			}
		}
		path := info.Inode.Filename
		//it should be inode here
		data, err := encodeInode(*info.Inode)
		if err != nil {
			p.unlock()
			return fmt.Errorf("err in enode inode")
		}
		_, state, err := p.Conn.Exists(path)
		if err != nil {
			p.unlock()
			return fmt.Errorf("unexpected err in zookeeper Exist")
		}
		_, err = p.Conn.Set(path, data, state.Version)
		if err != nil {
			p.unlock()
			return err
		}
	}
	// //clear fd
	p.ClientMtx.Lock()
	delete(p.info, fd)
	p.ClientMtx.Unlock()
	p.unlock()
	return nil
}

func (p *puddleStoreClient) Read(fd int, offset, size uint64) ([]byte, error) {
	//should return a copy of original data array
	p.ClientMtx.Lock()
	info, ok := p.info[fd]
	p.ClientMtx.Unlock()
	if !ok {
		return []byte{}, fmt.Errorf("invalid fd")
	}
	//handle edge case
	if info.Inode.Size == 0 {
		return []byte{}, nil
	}
	if offset > info.Inode.Size {
		// return []byte{}, fmt.Errorf("reach here, %v", info.Inode.Size)
		return []byte{}, nil
	}

	// data, err := p.readBlock(fd, 0)
	//calculate the blocks we need to read
	boundary := info.Inode.Size
	if offset+size <= boundary {
		boundary = offset + size
		// return nil, fmt.Errorf("reach 1 %v, %v, %v", boundary, offset, size)
	} else {
		// return nil, fmt.Errorf("reach 2 %v, %v, %v", boundary, offset, size)
		size = boundary - offset
		// return nil, fmt.Errorf("reach 2 %v, %v, %v", boundary, offset, size)
	}

	startBlock := offset / p.config.BlockSize
	endBlock := boundary / p.config.BlockSize
	offsetFirstBlock := offset % p.config.BlockSize
	if startBlock == endBlock {
		//first read from cache
		data, err := p.readBlock(fd, int(startBlock))
		if len(data) == 0 || err != nil {
			for i := 0; i < RETRY; i++ {
				data, err = p.readBlock(fd, int(startBlock))
				if err == nil && len(data) != 0 {
					break
				}
			}
			if err != nil || len(data) != 0 {
				return []byte{}, err
			}
		}
		rlt := data[offsetFirstBlock:(offsetFirstBlock + size)]
		// return nil, fmt.Errorf("%v", rlt)
		// return nil, fmt.Errorf("%v", data)
		// return nil, fmt.Errorf("reach here %v, %v, %v", offsetFirstBlock, size, data[offsetFirstBlock:(offsetFirstBlock+size)])
		//cache it
		return rlt, nil
	}
	var rlt []byte
	var bytesRead uint64
	bytesRead = 0
	for i := startBlock; i <= endBlock; i++ {
		//if cached, read locally
		data, err := p.readBlock(fd, int(i))
		if len(data) == 0 || err != nil {
			for i := 0; i < RETRY; i++ {
				data, err = p.readBlock(fd, int(i))
				if err == nil && len(data) != 0 {
					break
				}
			}
			if err != nil || len(data) != 0 {
				return []byte{}, err
			}
		}
		//cache it
		if i == startBlock {
			data = data[offsetFirstBlock:]
			bytesRead = bytesRead + uint64(len(data))
			// return nil, fmt.Errorf("%v, %v", bytesRead, uint64(len(data)))
		} else {
			if i == endBlock {
				left := size - bytesRead
				// return nil, fmt.Errorf("%v, %v", left, bytesRead)
				if left < p.config.BlockSize {
					data = data[:left]
				}
			} else {
				bytesRead = bytesRead + uint64(len(data))
			}
		}
		rlt = append(rlt, data...)
	}
	return rlt, nil
}

func (p *puddleStoreClient) Write(fd int, offset uint64, data []byte) error {
	//should return a copy of original data array and the client should be able to read its own write
	//if anything fail, should we clear fd?
	p.ClientMtx.Lock()
	// return fmt.Errorf("%v, %v, %v, %v", len(data), offset, p.config.BlockSize, data)
	info, ok := p.info[fd]
	p.ClientMtx.Unlock()
	if !ok {
		return fmt.Errorf("invalid fd")
	}
	//Writing to An Open File With write=false should return errors
	if !info.Flush {
		//should we unlock?
		return fmt.Errorf("write == false")
	}
	//handle edge case
	if len(data) == 0 {
		return nil
	}
	// if offset > info.Inode.Size, [info.Inode.Size, offset) should be filled with 0
	// write data []byte
	// for each block, salt p.config.NumReplicas times and publish it
	// make sure at least one is published
	// cache the write
	dataLen := len(data)
	currentBlock := info.Inode.Size / p.config.BlockSize
	startBlock := offset / p.config.BlockSize
	endBlock := (offset + uint64(len(data))) / p.config.BlockSize
	if offset > info.Inode.Size {
		// if offset > info.Inode.Size, [info.Inode.Size, offset) should be filled with 0
		if info.Inode.Size%p.config.BlockSize == 0 {
			p.savecache(fd, int(currentBlock), make([]byte, p.config.BlockSize))
		}
		for startBlock > currentBlock {
			currentBlock += 1
			p.savecache(fd, int(currentBlock), make([]byte, p.config.BlockSize))
		}
	}
	if startBlock == endBlock {
		tmp, err := p.readBlock(fd, int(startBlock))
		if err != nil {
			for i := 0; i < RETRY; i++ {
				tmp, err = p.readBlock(fd, int(startBlock))
				if err == nil {
					break
				}
			}
		}
		if err != nil {
			return err
		}
		if len(tmp) == 0 {
			tmp = make([]byte, p.config.BlockSize)
		}
		r := tmp[:offset%p.config.BlockSize]
		r = append(r, data...)
		r = append(r, tmp[(offset+uint64(len(data)))%p.config.BlockSize:]...)
		p.savecache(fd, int(startBlock), r)
	} else {
		tmp, err := p.readBlock(fd, int(startBlock))
		if err != nil {
			for i := 0; i < RETRY; i++ {
				tmp, err = p.readBlock(fd, int(startBlock))
				if err == nil {
					break
				}
			}
		}
		if err != nil {
			return err
		}
		if len(tmp) == 0 {
			tmp = make([]byte, p.config.BlockSize)
		}
		r := tmp[:offset%p.config.BlockSize]
		initbytes := p.config.BlockSize - offset%p.config.BlockSize
		r = append(r, data[:initbytes]...)
		p.savecache(fd, int(startBlock), r)
		data = data[initbytes:]
		for i := startBlock + 1; i < endBlock; i++ {
			r = data[:p.config.BlockSize]
			data = data[p.config.BlockSize:]
			p.savecache(fd, int(i), r)
		}
		tmp, err = p.readBlock(fd, int(endBlock))
		if err != nil {
			for i := 0; i < RETRY; i++ {
				tmp, err = p.readBlock(fd, int(startBlock))
				if err == nil {
					break
				}
			}
		}
		if err != nil {
			return err
		}
		if len(tmp) == 0 {
			tmp = make([]byte, p.config.BlockSize)
		}
		r = data[:]
		r = append(r, tmp[len(data):]...)
		p.savecache(fd, int(endBlock), r)
	}
	if offset+uint64(dataLen) > p.info[fd].Inode.Size {
		p.ClientMtx.Lock()
		p.info[fd].Inode.Size = offset + uint64(dataLen)
		p.ClientMtx.Unlock()
	}
	return nil
}

func (p *puddleStoreClient) savecache(fd, numBlock int, data []byte) {
	p.ClientMtx.Lock()
	filename := p.info[fd].Inode.Filename
	if p.cache[filename] == nil {
		p.cache[filename] = make(map[int][]byte)
	}
	p.cache[filename][numBlock] = data
	p.info[fd].Modified[numBlock] = true
	p.ClientMtx.Unlock()
}

func (p *puddleStoreClient) publish(fd int) error {
	p.ClientMtx.Lock()
	for numBlock := range p.info[fd].Modified {
		p.info[fd].Inode.Blocks[numBlock] = make([]string, 0)
		success := false
		p.ClientMtx.Unlock()
		taps, err := p.connectRemotes()
		if err != nil {
			return fmt.Errorf("need to republish")
		}
		p.ClientMtx.Lock()
		for _, tap := range taps {
			success = true
			filename := p.info[fd].Inode.Filename
			saltname := filename + "-" + strconv.Itoa(numBlock) + "-" + strconv.Itoa(p.getSeq())
			err = tap.Store(saltname, p.cache[filename][numBlock])
			if err == nil {
				p.info[fd].Inode.Blocks[numBlock] = append(p.info[fd].Inode.Blocks[numBlock], saltname)
			}
		}
		if !success {
			p.ClientMtx.Unlock()
			return fmt.Errorf("at least one block fails to publish")
		}
	}
	p.ClientMtx.Unlock()
	return nil
}

func (p *puddleStoreClient) readBlockTry(fd, numBlock int) ([]byte, error) {
	tmp, err := p.readBlock(fd, int(numBlock))
	if err != nil {
		for i := 0; i < RETRY; i++ {
			tmp, err = p.readBlock(fd, int(numBlock))
			if err == nil {
				break
			}
		}
	}
	return tmp, err
}

func (p *puddleStoreClient) readBlock(fd, numBlock int) ([]byte, error) {
	p.ClientMtx.Lock()
	filename := p.info[fd].Inode.Filename
	data, ok := p.cache[filename][numBlock]
	p.ClientMtx.Unlock()
	if ok {
		return data, nil
	} else {
		remote, err := p.connectRemote()
		if err != nil {
			return []byte{}, fmt.Errorf("problem in connectRemote, %v", err)
		}
		rawdata, _, err := p.Conn.Get(p.info[fd].Inode.Filename)
		if err != nil {
			return []byte{}, fmt.Errorf("get inode, filename %v, %v", p.info[fd].Inode.Filename, err)
		}
		node, err := decodeInode(rawdata)
		if err != nil {
			return []byte{}, err
		}
		for _, saltname := range node.Blocks[numBlock] {
			data, err = remote.Get(saltname)
			if err != nil {
				continue
			}
			return data, nil
		}
		return []byte{}, nil
	}
}

func (p *puddleStoreClient) Mkdir(path string) error {
	if path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}
	if len(path) == 0 {
		return fmt.Errorf("not allowed to create mkdir of root")
	}
	if underFile, err := p.underFile(path); underFile || err != nil {
		return fmt.Errorf("create under file %v", path)
	}
	node := inode{
		IsDir:    true,
		Filename: path,
	}
	data, err := encodeInode(node)
	if err != nil {
		return err
	}
	acl := zk.WorldACL(zk.PermAll)
	//lock
	_, err = p.Conn.Create(path, data, 0, acl)
	//unlock
	return err
}

func (p *puddleStoreClient) Remove(path string) error {
	//lock is not required for Remove
	//if it is a dir, it should recursively remove its descendents
	exist, err := p.isFileExist(path)
	if err != nil {
		return err
	}
	if !exist {
		return fmt.Errorf("not exist")
	}
	data, state, err := p.Conn.Get(path)
	if err != nil {
		return err
	}
	if path != "/tapestry" {
		node, err := decodeInode(data)
		if err != nil {
			return err
		}
		if node.IsDir {
			children, _, err := p.Conn.Children(path)
			if err != nil {
				return err
			}
			for _, s := range children {
				p.Remove(s)
			}
			p.Conn.Delete(path, state.Version) //version
		} else {
			p.Conn.Delete(path, state.Version)
		}
	} else {
		p.Conn.Delete(path, state.Version)
	}

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
	err := fmt.Errorf("")
	for i := 0; i < RETRY; i += 1 {
		rand.Seed(time.Now().UnixNano())
		p.ClientMtx.Lock()
		if len(p.children) == 0 {
			p.ClientMtx.Unlock()
			return nil, fmt.Errorf("no child")
		}
		path := "/tapestry/" + p.children[rand.Intn(len(p.children))]
		addr, _, err := p.Conn.Get(path)
		if err != nil {
			continue
		}
		remote, err := tapestry.Connect(string(addr))
		if err != nil {
			continue
		}
		p.ClientMtx.Unlock()
		return remote, err
	}
	p.ClientMtx.Unlock()
	return nil, err
}

func (p *puddleStoreClient) watch() {
	children, _, eventChan, _ := p.Conn.ChildrenW("/tapestry")
	p.ClientMtx.Lock()
	p.children = children
	p.ClientMtx.Unlock()
	for {
		event := <-eventChan
		p.ClientMtx.Lock()
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
		p.ClientMtx.Unlock()
	}
}

func (p *puddleStoreClient) shuffleChildren() (subnames []string) {
	rand.Seed(time.Now().UnixNano())
	p.ClientMtx.Lock()
	for i := len(p.children) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		p.children[i], p.children[j] = p.children[j], p.children[i]
	}
	if len(p.children) < p.config.NumReplicas {
		subnames = p.children[:]
	} else {
		subnames = p.children[:p.config.NumReplicas]
	}
	p.ClientMtx.Unlock()
	return subnames
}

func (p *puddleStoreClient) connectRemotes() ([]*tapestry.Client, error) {
	//load balancing: It's better to have client gets a random node each time when need to interact.
	//choose  random remote paths from p.children

	////retry!!!!!!!
	var remotes []*tapestry.Client
	subnames := p.shuffleChildren()
	for _, subname := range subnames {
		path := "/tapestry/" + subname
		addr, _, err := p.Conn.Get(path)
		if err != nil {
			continue
		}
		remote, err := tapestry.Connect(string(addr))
		if err == nil {
			remotes = append(remotes, remote)
		}
	}
	return remotes, nil
}
