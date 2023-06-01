package my_lsm

import (
	"log"
	"my_lsm/config"
)

// Start 启动数据库
func Start(conf config.Config) {
	config.Init(conf)

	if err := InitDatabase(conf.DataDir); err != nil {
		log.Println(err)
		return
	}

	checkMemory()

	database.TableTree.Check()

	go Check()
}
