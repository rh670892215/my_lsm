package sstable

import (
	"os"
	"sync"
)

/*

索引是从数据区开始！
0 ─────────────────────────────────────────────────────────►
◄───────────────────────────
          dataLen          ◄──────────────────
                                indexLen     ◄──────────────┐
┌──────────────────────────┬─────────────────┬──────────────┤
│                          │                 │              │
│          数据区           │   稀疏索引区     │    元数据     │
│                          │                 │              │
└──────────────────────────┴─────────────────┴──────────────┘
*/

// SSTable 文件存储的最小单元SSTable
type SSTable struct {
	// 文件相关属性
	F        *os.File
	FilePath string

	// 稀疏索引列表
	IndexRegion map[string]Position
	// 排序后的key列表
	SortedIndex []string
	// 元数据
	MetaRegion MetaData
	// SSTable 只能使排他锁
	Lock sync.Locker
}

// MetaData 元数据
type MetaData struct {
	Version          int64
	DataRegionStart  int64
	DataRegionLen    int64
	IndexRegionStart int64
	IndexRegionLen   int64
}

// Position 位置信息
type Position struct {
	Start    int64
	Len      int64
	IsDelete bool
}
