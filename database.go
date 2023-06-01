package my_lsm

import (
	"log"
	"my_lsm/sorted_tree"
	"my_lsm/ssTable/sstable_tree"
	"my_lsm/wal"
	"os"
)

// Database 数据模型
type Database struct {
	// 内存表
	MemoryTree *sorted_tree.SortedTree
	// SSTable 列表
	TableTree *sstable_tree.TableTree
	// WalF 文件句柄
	Wal *wal.Wal
}

// 数据库，全局唯一实例
var database *Database

// InitDatabase 初始化
func InitDatabase(dir string) error {
	database = &Database{
		MemoryTree: &sorted_tree.SortedTree{},
		TableTree:  &sstable_tree.TableTree{},
		Wal:        &wal.Wal{},
	}

	// 若文件不存在
	if _, err := os.Stat(dir); err != nil {
		log.Printf("The %s directory does not exist. The directory is being created\r\n", dir)
		err := os.Mkdir(dir, 0666)
		if err != nil {
			log.Println("Failed to create the database directory")
			return err
		}
	}

	//
	memoryTree, err := database.Wal.Init(dir)
	if err != nil {
		log.Printf("Failed to init wal error.err :%v\n", err)
		return err
	}
	database.MemoryTree = memoryTree
	database.TableTree.Init(dir)
	return nil

}
