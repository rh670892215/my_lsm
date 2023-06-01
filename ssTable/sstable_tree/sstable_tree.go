package sstable_tree

import (
	"io/ioutil"
	"log"
	"my_lsm/config"
	"my_lsm/entity"
	"path"
	"sync"
)

// Init 初始化TableTree
func (t *TableTree) Init(dir string) {
	conf := config.GetConfig()
	t.levels = make([]*TreeNode, conf.MaxLevel)
	t.locker = &sync.RWMutex{}
	// 读取目录下所有的文件
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Println("Failed to read the database file")
	}

	for _, info := range infos {
		if path.Ext(info.Name()) == ".db" {
			if err := t.LoadDBFile(path.Join(dir, info.Name())); err != nil {
				log.Printf("LoadDBFile error err :%v\n", err)
			}
		}
	}
}

// Search TableTree查找指定key
func (t *TableTree) Search(key string) (*entity.AtomicData, entity.SearchResult) {
	t.locker.RLock()
	defer t.locker.RUnlock()

	for _, treeNode := range t.levels {
		// 构建当前层级的treeNode列表，方便后续倒序查找
		curNodes := make([]*TreeNode, 0)
		for treeNode != nil {
			curNodes = append(curNodes, treeNode)
			treeNode = treeNode.Next
		}

		for i := len(curNodes) - 1; i >= 0; i-- {
			value, result := curNodes[i].Table.Search(key)
			if result == entity.NotExist {
				continue
			}
			return value, result
		}
	}
	return nil, entity.NotExist
}

// CreateNewSSTable 创建新的SSTable节点
func (t *TableTree) CreateNewSSTable(atomicDatas []*entity.AtomicData) error {
	return t.insertNewTreeNode(atomicDatas, 0)
}
