package main

import (
	"bufio"
	"fmt"
	"my_lsm"
	"my_lsm/config"
	"os"
	"time"
)

type TestValue struct {
	A int64
	B int64
	C int64
	D string
}

func main() {
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println(r)
			inputReader := bufio.NewReader(os.Stdin)
			_, _ = inputReader.ReadString('\n')
		}
	}()
	// 生成levelSize
	levelSize := make([]int, 10)
	levelSize[0] = 100
	for i := 1; i < 10; i++ {
		levelSize[i] = 10 * levelSize[i-1]
	}

	my_lsm.Start(config.Config{
		DataDir:       `/Users/renhang/github-golang/my_lsm/datas`,
		LevelSize:     levelSize,
		PartSize:      4,
		Threshold:     3000,
		CheckInterval: 3,
		MaxLevel:      10,
	})

	query()
	//myTest()
	//insert()
	//query()

}

func myTest() {
	testVa := TestValue{
		A: 1,
		B: 1,
		C: 1,
		D: "00000000000000000000000000000000000000",
	}

	testVb := TestValue{
		A: 2,
		B: 2,
		C: 2,
		D: "00000000000000000000000000000000000000",
	}

	testVc := TestValue{
		A: 3,
		B: 4,
		C: 5,
		D: "00000000000000000000000000000000000000",
	}

	my_lsm.Set(string("a"), testVa)
	my_lsm.Set(string("b"), testVb)
	my_lsm.Set(string("c"), testVc)

	v, _ := my_lsm.Get[TestValue]("a")
	fmt.Println(v)
	v, _ = my_lsm.Get[TestValue]("b")
	fmt.Println(v)
	v, _ = my_lsm.Get[TestValue]("c")
	fmt.Println(v)
}

func query() {
	start := time.Now()
	v, ok := my_lsm.Get[TestValue]("aaaaaa")
	elapse := time.Since(start)
	fmt.Println("查找 aaaaaa 完成，消耗时间：", elapse)
	fmt.Println(v)
	fmt.Println(ok)

	start = time.Now()
	v, ok = my_lsm.Get[TestValue]("aaabbb")
	elapse = time.Since(start)
	fmt.Println("查找 aaabbb 完成，消耗时间：", elapse)
	fmt.Println(v)
	fmt.Println(ok)
}
func insert() {

	// 64 个字节
	testV := TestValue{
		A: 1,
		B: 1,
		C: 3,
		D: "00000000000000000000000000000000000000",
	}

	count := 0
	start := time.Now()
	key := []byte{'a', 'a', 'a', 'a', 'a', 'a'}
	my_lsm.Set(string(key), testV)
	for a := 0; a < 26; a++ {
		for b := 0; b < 26; b++ {
			for c := 0; c < 26; c++ {
				for d := 0; d < 26; d++ {
					for e := 0; e < 26; e++ {
						for f := 0; f < 26; f++ {
							key[0] = 'a' + byte(a)
							key[1] = 'a' + byte(b)
							key[2] = 'a' + byte(c)
							key[3] = 'a' + byte(d)
							key[4] = 'a' + byte(e)
							key[5] = 'a' + byte(f)
							my_lsm.Set(string(key), testV)
							count++
						}
					}
				}
			}
		}
	}
	elapse := time.Since(start)
	fmt.Println("插入完成，数据量：", count, ",消耗时间：", elapse)
}
