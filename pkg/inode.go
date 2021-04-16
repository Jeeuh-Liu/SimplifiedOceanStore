package pkg

type inode struct {
	IsDir    bool
	Filename string
	Size     uint64
	Blocks   map[int][]string //block number to GUID?
}
