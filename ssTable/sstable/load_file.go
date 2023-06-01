package sstable

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"sort"
	"sync"
)

// Init 初始化SSTable
func (s *SSTable) Init(path string) error {
	s.FilePath = path
	s.Lock = &sync.Mutex{}

	return s.loadFile()
}

// 加载文件句柄
func (s *SSTable) loadFile() error {
	if s.F == nil {
		f, err := os.OpenFile(s.FilePath, os.O_RDONLY, 0666)
		if err != nil {
			return err
		}
		s.F = f
	}
	if err := s.loadMetaRegion(); err != nil {
		return err
	}
	if err := s.loadIndexRegion(); err != nil {
		return err
	}
	return nil
}

// 加载元数据
func (s *SSTable) loadMetaRegion() error {
	if _, err := s.F.Seek(0, 0); err != nil {
		return err
	}
	fileInfo, err := s.F.Stat()
	if err != nil {
		return err
	}

	if _, err = s.F.Seek(fileInfo.Size()-8*5, 0); err != nil {
		return err
	}
	if err = binary.Read(s.F, binary.LittleEndian, &s.MetaRegion.Version); err != nil {
		return err
	}

	if _, err = s.F.Seek(fileInfo.Size()-8*4, 0); err != nil {
		return err
	}
	if err = binary.Read(s.F, binary.LittleEndian, &s.MetaRegion.DataRegionStart); err != nil {
		return err
	}

	if _, err = s.F.Seek(fileInfo.Size()-8*3, 0); err != nil {
		return err
	}
	if err = binary.Read(s.F, binary.LittleEndian, &s.MetaRegion.DataRegionLen); err != nil {
		return err
	}

	if _, err = s.F.Seek(fileInfo.Size()-8*2, 0); err != nil {
		return err
	}
	if err = binary.Read(s.F, binary.LittleEndian, &s.MetaRegion.IndexRegionStart); err != nil {
		return err
	}

	if _, err = s.F.Seek(fileInfo.Size()-8*1, 0); err != nil {
		return err
	}
	if err = binary.Read(s.F, binary.LittleEndian, &s.MetaRegion.IndexRegionLen); err != nil {
		return err
	}

	return nil
}

// 加载稀疏索引
func (s *SSTable) loadIndexRegion() error {
	bytes := make([]byte, s.MetaRegion.IndexRegionLen)
	if _, err := s.F.Seek(s.MetaRegion.IndexRegionStart, 0); err != nil {
		return err
	}

	if _, err := s.F.Read(bytes); err != nil {
		return err
	}

	s.IndexRegion = make(map[string]Position)
	if err := json.Unmarshal(bytes, &s.IndexRegion); err != nil {
		return err
	}

	// 重新设置指针到文件起始位置
	_, _ = s.F.Seek(0, 0)

	keyList := make([]string, 0, len(s.IndexRegion))
	for key, _ := range s.IndexRegion {
		keyList = append(keyList, key)
	}

	sort.Strings(keyList)
	s.SortedIndex = keyList
	return nil
}

// GetDBSize 获取SSTable中的文件大小
func (s *SSTable) GetDBSize() int64 {
	info, err := os.Stat(s.FilePath)
	if err != nil {
		log.Printf("SSTable GetDBSize err. err: %v", err)
		return 0
	}
	return info.Size()
}
