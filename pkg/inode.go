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
