package sstable

import (
	"log"
	"my_lsm/entity"
)

// Search 根据key查找数据
func (s *SSTable) Search(key string) (*entity.AtomicData, entity.SearchResult) {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	// todo 这里写完之后测试一下如果按顺序直接存key-position，直接构建成结构体，不用map会不会更高效
	// 这里直接序列化了map，存入文件，所以直接通过map拿数据就可以了
	data, ok := s.IndexRegion[key]
	if !ok {
		return nil, entity.NotExist
	}

	// 读取文件里面的数据，数据是按序存储的，有利于顺序IO
	start := data.Start
	dataLen := data.Len

	bytes := make([]byte, dataLen)
	if _, err := s.F.Seek(start, 0); err != nil {
		log.Printf("SSTable Search agjust file point error. err : %v", err)
		return nil, entity.NotExist
	}

	if _, err := s.F.Read(bytes); err != nil {
		log.Printf("SSTable Search read file error. err : %v", err)
		return nil, entity.NotExist
	}

	atomicData, err := entity.Decode(bytes)
	if err != nil {
		log.Printf("SSTable Search decode atomicData error. err : %v", err)
		return nil, entity.NotExist
	}

	if atomicData.IsDeleted {
		return atomicData, entity.Deleted
	}
	return atomicData, entity.Exist
}
