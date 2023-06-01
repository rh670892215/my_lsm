package sstable_tree

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"my_lsm/ssTable/sstable"
	"os"
	"sort"
	"strconv"
	"sync"

	"my_lsm/config"
	"my_lsm/entity"
	"my_lsm/sorted_tree"
)

// Check 检查是否需要进行压缩
func (t *TableTree) Check() {
	// 校验每一层的节点数量、大小是否超过阈值
	conf := config.GetConfig()
	for level, _ := range t.levels {
		tableNum := t.GetLevelNum(level)
		tableSize := int(t.GetLevelSize(level) / 1000 / 1000) // 转为 MB

		if tableNum > conf.PartSize || tableSize > conf.LevelSize[level] {
			if err := t.majorCompactionLevel(level); err != nil {
				log.Printf("Check merge TableTree error .err : %v", err)
			}
		}
	}
}

// GetLevelSize 获取指定层级的文件总大小
func (t *TableTree) GetLevelSize(level int) int64 {
	var totalSize int64
	node := t.levels[level]
	for node != nil {
		totalSize += node.Table.GetDBSize()
		node = node.Next
	}
	return totalSize
}

// GetLevelNum 获取指定层级的节点数量
func (t *TableTree) GetLevelNum(level int) int {
	var totalNum int
	node := t.levels[level]
	for node != nil {
		totalNum++
		node = node.Next
	}
	return totalNum
}

// 压缩当前层的文件到下一层
func (t *TableTree) majorCompactionLevel(level int) error {
	// 将当前层的数据聚合到一颗memory tree中
	curNode := t.levels[level]
	memoryTree := sorted_tree.NewSortedTree()
	tableCache := make([]byte, config.GetConfig().LevelSize[level])
	t.locker.Lock()
	for curNode != nil {
		curTable := curNode.Table
		// 将 SSTable 的数据区加载到 tableCache 内存中
		if int64(len(tableCache)) < curTable.MetaRegion.DataRegionLen {
			tableCache = make([]byte, curTable.MetaRegion.DataRegionLen)
		}
		if curTable.F == nil {
			curTable.F, _ = os.OpenFile(curTable.FilePath, os.O_RDONLY, 0666)
		}
		if _, err := curTable.F.Seek(0, 0); err != nil {
			return err
		}
		if _, err := curTable.F.Read(tableCache); err != nil {
			return err
		}

		// 逐个元素读取
		for key, val := range curTable.IndexRegion {
			if val.IsDelete {
				memoryTree.Remove(key)
				continue
			}
			atomicData, err := entity.Decode(tableCache[val.Start : val.Start+val.Len])
			if err != nil {
				return err
			}
			memoryTree.Insert(key, atomicData.Value)
		}
		curNode = curNode.Next
	}
	t.locker.Unlock()

	values := memoryTree.GetValues()
	newLevel := level + 1
	if newLevel > config.GetConfig().MaxLevel {
		newLevel = config.GetConfig().MaxLevel
	}
	if err := t.insertNewTreeNode(values, newLevel); err != nil {
		return err
	}
	t.clearLevel(level)
	return nil
}

// 清理指定层级的节点
func (t *TableTree) clearLevel(level int) {
	t.locker.Lock()
	defer t.locker.Unlock()

	node := t.levels[level]
	for node != nil {
		node.Table.F.Close()
		os.Remove(node.Table.FilePath)
		node.Table.F = nil
		node.Table = nil
		node = node.Next
	}
	t.levels[level] = nil
}

// insertNewSSTable 在TableTree插入新的treeNode
func (t *TableTree) insertNewTreeNode(atomicDatas []*entity.AtomicData, level int) error {
	// 构建原子数据
	keys := make([]string, 0, len(atomicDatas))
	positionTable := make(map[string]sstable.Position)
	var dataRegion []byte

	for _, data := range atomicDatas {
		bytes, err := entity.Encode(data)
		if err != nil {
			log.Printf("insertNewSSTable encode data err.data :%+v, err: %v", data, err)
			return err
		}

		keys = append(keys, data.Key)
		positionTable[data.Key] = sstable.Position{
			Start:    int64(len(dataRegion)),
			Len:      int64(len(bytes)),
			IsDelete: data.IsDeleted,
		}
		dataRegion = append(dataRegion, bytes...)
	}
	sort.Strings(keys)

	// 构建索引区域
	indexRegion, err := json.Marshal(positionTable)
	if err != nil {
		log.Printf("insertNewSSTable encode positionTable err.data :%+v, err: %v", positionTable, err)
		return err
	}

	// 构建元数据
	metaInfo := sstable.MetaData{
		Version:          0,
		DataRegionStart:  0,
		DataRegionLen:    int64(len(dataRegion)),
		IndexRegionStart: int64(len(dataRegion)),
		IndexRegionLen:   int64(len(indexRegion)),
	}

	// 构建一个SSTable
	ssTable := &sstable.SSTable{
		MetaRegion:  metaInfo,
		SortedIndex: keys,
		IndexRegion: positionTable,
		Lock:        &sync.RWMutex{},
	}

	index := t.insert(ssTable, level)

	conf := config.GetConfig()
	filePath := conf.DataDir + "/" + strconv.Itoa(level) + "_" + strconv.Itoa(index) + ".db"
	ssTable.FilePath = filePath

	if err = writeData2File(filePath, dataRegion, indexRegion, metaInfo); err != nil {
		log.Printf("writeData2File err: %v", err)
		return err
	}

	// 打开刚写入的文件，给ssTable的F文件描述符赋值
	f, err := os.OpenFile(ssTable.FilePath, os.O_RDONLY, 0666)
	if err != nil {
		log.Println(" error open file ", ssTable.FilePath)
		panic(err)
	}
	ssTable.F = f
	return nil
}

// Insert 插入一个SSTable到指定层级，返回该sstable在本层的位置，用来生成数据文件名称
func (t *TableTree) insert(table *sstable.SSTable, level int) int {
	t.locker.Lock()
	defer t.locker.Unlock()

	if level >= len(t.levels) {
		return -1
	}
	newNode := &TreeNode{
		Table: table,
		Next:  nil,
		Index: 0,
	}
	treeNodes := t.levels[level]
	if treeNodes == nil {
		t.levels[level] = newNode
	}
	for treeNodes != nil {
		if treeNodes.Next == nil {
			newNode.Index = treeNodes.Index + 1
			treeNodes.Next = newNode
			break
		}
		treeNodes = treeNodes.Next

	}
	return newNode.Index
}

// 写入数据到磁盘
func writeData2File(filePath string, dataRegion []byte, indexRegin []byte, metaInfo sstable.MetaData) error {
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	defer f.Close()
	if err != nil {
		return err
	}

	if _, err = f.Write(dataRegion); err != nil {
		return err
	}
	if _, err = f.Write(indexRegin); err != nil {
		return err
	}

	// 写入元数据
	err = binary.Write(f, binary.LittleEndian, &metaInfo.Version)
	err = binary.Write(f, binary.LittleEndian, &metaInfo.DataRegionStart)
	err = binary.Write(f, binary.LittleEndian, &metaInfo.DataRegionLen)
	err = binary.Write(f, binary.LittleEndian, &metaInfo.IndexRegionStart)
	err = binary.Write(f, binary.LittleEndian, &metaInfo.IndexRegionLen)
	if err != nil {
		return err
	}
	// 刷盘
	if err = f.Sync(); err != nil {
		return err
	}
	return nil
}
