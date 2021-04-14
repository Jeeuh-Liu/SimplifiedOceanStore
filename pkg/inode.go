package pkg

import (
	tapestry "tapestry/pkg"
)

type Inode struct {
	IsDir    bool
	Filename string
	Size     uint64
	Blocks   map[int]tapestry.ID //block number to GUID?
}
