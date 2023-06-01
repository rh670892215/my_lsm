package sstable_tree

import (
	"fmt"
	"my_lsm/ssTable/sstable"
)

// LoadDBFile 加载一个db文件到TableTree
func (t *TableTree) LoadDBFile(filePath string) error {
	level, index, err := getLevel(filePath)
	if err != nil {
		return err
	}
	ssTable := &sstable.SSTable{}
	if err := ssTable.Init(filePath); err != nil {
		return err
	}
	newTreeNode := &TreeNode{
		Index: index,
		Table: ssTable,
	}

	curTreeNode := t.levels[level]

	// 当前层级是空，newNode直接做起始节点
	if curTreeNode == nil {
		t.levels[level] = newTreeNode
		return nil
	}

	// newNode节点的index小于当前的起始节点，直接插入在前面
	if newTreeNode.Index < curTreeNode.Index {
		newTreeNode.Next = curTreeNode
		t.levels[level] = newTreeNode
		return nil
	}

	// 将newNode插入到指定位置
	for curTreeNode != nil {
		if curTreeNode.Next == nil || newTreeNode.Index < curTreeNode.Index {
			newTreeNode.Next = curTreeNode
			t.levels[level] = newTreeNode
			return nil
		}
		curTreeNode = curTreeNode.Next
	}

	return nil
}

// 获取一个 db 文件所代表的 SSTable 的所在层数和索引
func getLevel(name string) (level int, index int, err error) {
	n, err := fmt.Sscanf(name, "/Users/renhang/github-golang/my_lsm/datas/%d_%d.db", &level, &index)
	if n != 2 || err != nil {
		return 0, 0, fmt.Errorf("incorrect data file name: %q", name)
	}
	return level, index, nil
}
