package sstable_tree

import (
	"my_lsm/ssTable/sstable"
	"sync"
)

// TableTree SSTable树
type TableTree struct {
	levels []*TreeNode
	locker *sync.RWMutex
}

// TreeNode SSTable树节点
type TreeNode struct {
	Index int
	Next  *TreeNode
	Table *sstable.SSTable
}
