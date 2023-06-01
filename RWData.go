package my_lsm

import (
	"encoding/json"
	"log"
	"my_lsm/entity"
)

// Get 获取一个元素
func Get[T any](key string) (T, bool) {
	var nilV T
	// 先查内存树
	val, res := database.MemoryTree.Search(key)
	if res == entity.Exist {
		instance, err := getInstance[T](val.Value)
		if err != nil {
			log.Printf("Get data error.err :%v", err)
			return nilV, false
		}
		return instance, true
	}

	// 查SSTable文件
	if database.TableTree == nil {
		return nilV, false
	}
	val, res = database.TableTree.Search(key)
	if res == entity.Exist {
		instance, err := getInstance[T](val.Value)
		if err != nil {
			log.Printf("Get data error.err :%v", err)
			return nilV, false
		}
		return instance, true
	}
	return nilV, false
}

// Set 插入元素
func Set[T any](key string, val T) bool {
	log.Print("Insert ", key, ",")
	bytes, err := json.Marshal(val)
	if err != nil {
		log.Printf("Set data write MemoryTree error.err :%v", err)
		return false
	}

	// 写入内存表
	database.MemoryTree.Insert(key, bytes)

	// 写入wal日志
	err = database.Wal.Write(&entity.AtomicData{
		Key:       key,
		Value:     bytes,
		IsDeleted: false,
	})
	if err != nil {
		log.Printf("Set data write wal error.err :%v", err)
		return false
	}
	return true
}

// Delete 删除元素
func Delete(key string) {
	database.MemoryTree.Remove(key)
	// 写入wal日志
	err := database.Wal.Write(&entity.AtomicData{
		Key:       key,
		Value:     nil,
		IsDeleted: false,
	})
	if err != nil {
		log.Printf("Delete data write wal error.err :%v", err)
	}

}

// 解析字节数组
func getInstance[T any](bytes []byte) (T, error) {
	var res T
	if err := json.Unmarshal(bytes, &res); err != nil {
		return res, err
	}
	return res, nil
}
