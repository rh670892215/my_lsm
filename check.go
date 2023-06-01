package my_lsm

import (
	"log"
	"my_lsm/config"
	"time"
)

// Check 定时检查
func Check() {
	checkTime := config.GetConfig().CheckInterval
	ticker := time.Tick(time.Duration(checkTime) * time.Second)
	for range ticker {
		log.Println("Performing background checks...")

		checkMemory()
		database.TableTree.Check()
	}
}

// 检查内存是否需要写入磁盘
func checkMemory() {
	threshold := config.GetConfig().Threshold
	count := database.MemoryTree.GetCount()
	if count < threshold {
		return
	}
	// 写入磁盘
	oldTree := database.MemoryTree.Swap()
	if err := database.TableTree.CreateNewSSTable(oldTree.GetValues()); err != nil {
		log.Printf("checkMemory CreateNewSSTable error.err : %v\n", err)
		return
	}
	// 重置日志
	database.Wal.Reset()
}
