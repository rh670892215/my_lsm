package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"path"
	"sync"

	"my_lsm/entity"
	"my_lsm/sorted_tree"
)

// Wal 日志结构
type Wal struct {
	f        *os.File
	filePath string
	lock     sync.Locker
}

// Init 初始化
func (w *Wal) Init(dir string) (*sorted_tree.SortedTree, error) {
	walPath := path.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	w.lock = &sync.Mutex{}
	w.f = f
	w.filePath = walPath
	return w.loadMemory()
}

// loadMemory 加载log到内存表
func (w *Wal) loadMemory() (*sorted_tree.SortedTree, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	info, err := w.f.Stat()
	if err != nil {
		return nil, err
	}

	sortedTree := sorted_tree.NewSortedTree()

	totalSize := info.Size()
	if totalSize == 0 {
		log.Println("wal log file is empty")
		return sortedTree, nil
	}

	if _, err = w.f.Seek(0, 0); err != nil {
		return nil, err
	}
	// 文件指针移动到最后，以便追加
	defer func(f *os.File, offset int64, whence int) {
		_, err := f.Seek(offset, whence)
		if err != nil {
			log.Println("Failed to open the wal.log")
		}
	}(w.f, totalSize-1, 0)

	totalData := make([]byte, totalSize)
	if _, err = w.f.Read(totalData); err != nil {
		return nil, err
	}

	dataLen := int64(0)
	index := int64(0)

	for index < totalSize {
		indexData := totalData[index : index+8]
		buf := bytes.NewBuffer(indexData)
		if err = binary.Read(buf, binary.LittleEndian, &dataLen); err != nil {
			log.Println("Failed to open the wal.log")
			continue
		}
		index += 8

		dataRegion := totalData[index : index+dataLen]
		// 读取下一个元素
		index += dataLen
		var atomicData entity.AtomicData
		if err = json.Unmarshal(dataRegion, &atomicData); err != nil {
			log.Println("Failed to open the wal.log")
			continue
		}

		if atomicData.IsDeleted {
			sortedTree.Remove(atomicData.Key)
			continue
		}
		sortedTree.Insert(atomicData.Key, atomicData.Value)
	}

	return sortedTree, nil
}

// Write 操作记录写入log
func (w *Wal) Write(data *entity.AtomicData) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if err = binary.Write(w.f, binary.LittleEndian, int64(len(jsonData))); err != nil {
		return err
	}
	if err = binary.Write(w.f, binary.LittleEndian, jsonData); err != nil {
		return err
	}
	return nil
}

// Reset 重置日志
func (w *Wal) Reset() {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.f.Close()
	w.f = nil

	os.Remove(w.filePath)
	f, err := os.OpenFile(w.filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {

	}
	w.f = f
}
