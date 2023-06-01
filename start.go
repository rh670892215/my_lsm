package my_lsm

import "my_lsm/config"

// Start 启动数据库
func Start(conf config.Config) {
	config.Init(conf)

	InitDatabase(conf.DataDir)

	checkMemory()

	database.TableTree.Check()

	go Check()
}
