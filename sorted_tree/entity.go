package sorted_tree

import (
	"my_lsm/entity"
	"sync"
)

// TreeNode 树节点
type TreeNode struct {
	Left  *TreeNode
	Right *TreeNode
	Data  *entity.AtomicData
}

// SortedTree 二叉排序树
type SortedTree struct {
	Root           *TreeNode
	count          int
	rWLock         *sync.RWMutex
	sortedTreeImpl sortedTreeImpl
}

// todo key改成any
type SortedTreeOrder struct {
	SortedTree
}

type sortedTreeImpl interface {
	find(key string) *TreeNode
}
