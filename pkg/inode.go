package pkg

import (
	tapestry "tapestry/pkg"
)

type inode struct {
	IsDir    bool
	Filename string
	Size     uint64
	Blocks   map[int]tapestry.ID //block number to GUID?
}

//initially I bet the file should have at least one block
func newInode() inode {
}
