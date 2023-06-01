package sorted_tree

import (
	"log"
	"sync"

	"my_lsm/entity"
)

// NewTreeNode 初始化TreeNode
func NewTreeNode(key string, value []byte) *TreeNode {
	return &TreeNode{
		Data: &entity.AtomicData{
			Key:       key,
			Value:     value,
			IsDeleted: false,
		},
	}
}

// NewSortedTree 初始化SortedTree
func NewSortedTree() *SortedTree {
	so := SortedTreeOrder{}
	so.init()
	so.sortedTreeImpl = &so
	return &so.SortedTree
}

// SortedTree必要字段初始化
func (s *SortedTree) init() {
	s.rWLock = &sync.RWMutex{}
}

// Search 查找节点
func (s *SortedTree) Search(key string) (*entity.AtomicData, entity.SearchResult) {
	s.rWLock.RLock()
	defer s.rWLock.RUnlock()

	targetNode := s.sortedTreeImpl.find(key)
	if targetNode == nil {
		return nil, entity.NotExist
	}

	if targetNode.Data.IsDeleted {
		return targetNode.Data, entity.Deleted
	}

	return targetNode.Data, entity.Exist
}

// Insert 插入节点
func (s *SortedTree) Insert(key string, value []byte) (*entity.AtomicData, bool) {
	s.rWLock.Lock()
	s.rWLock.Unlock()

	if s.Root == nil {
		s.Root = NewTreeNode(key, value)
		s.count++
		return s.Root.Data, true
	}

	currentNode := s.Root
	for currentNode != nil {
		if currentNode.Data.Key == key {
			currentNode.Data.Value = value
			currentNode.Data.IsDeleted = false

			return currentNode.Data, true
		}

		if key < currentNode.Data.Key {
			if currentNode.Left == nil {
				currentNode.Left = NewTreeNode(key, value)
				s.count++
				return currentNode.Left.Data, false
			}

			currentNode = currentNode.Left
			continue
		}
		if currentNode.Right == nil {
			currentNode.Right = NewTreeNode(key, value)
			s.count++
			return currentNode.Right.Data, false
		}
		currentNode = currentNode.Right
	}
	log.Fatalf("The tree fail to Set value, key: %s, value: %v", key, value)
	return nil, false
}

// Remove 移除节点
func (s *SortedTree) Remove(key string) bool {
	s.rWLock.Lock()
	defer s.rWLock.Unlock()

	targetNode := s.sortedTreeImpl.find(key)
	if targetNode == nil {
		return false
	}
	s.count--
	targetNode.Data.IsDeleted = true
	return true
}

// GetValues 获取排序后的AtomicData，中序遍历
func (s *SortedTree) GetValues() []*entity.AtomicData {
	s.rWLock.RLock()
	defer s.rWLock.RUnlock()

	var res []*entity.AtomicData
	node := s.Root
	stack := InitStack(0)
	for {
		if node != nil {
			stack.Push(node)
			node = node.Left
			continue
		}
		tmpNode, ok := stack.Pop()
		if !ok {
			break
		}
		res = append(res, tmpNode.Data)
		node = tmpNode.Right
	}
	return res
}

// GetCount 获取节点数量
func (s *SortedTree) GetCount() int {
	return s.count
}

// Swap 交换出之前的旧树
func (s *SortedTree) Swap() *SortedTree {
	s.rWLock.Lock()
	defer s.rWLock.Unlock()

	newTree := NewSortedTree()
	newTree.Root = s.Root
	s.Root = nil
	s.count = 0
	return newTree
}

func (so *SortedTreeOrder) find(key string) *TreeNode {
	if so.Root == nil {
		return nil
	}
	currentNode := so.Root
	for currentNode != nil {
		if key == currentNode.Data.Key {
			return currentNode
		}

		if key < currentNode.Data.Key {
			currentNode = currentNode.Left
			continue
		}
		currentNode = currentNode.Right
	}
	return nil
}
