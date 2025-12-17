package main

import (
	"flag"
	"fmt"
	"github.com/aemonswift01/hack-one/graph"
	"os"
	"time"
)

func main() {
	// 解析参数
	csvPath := flag.String("f", "", "CSV文件路径（必填）")
	memLimitMB := flag.Uint64("m", 6144, "内存限制（MB），默认6144")
	flag.Parse()

	if *csvPath == "" {
		fmt.Println("错误：必须指定CSV文件路径（-f参数）")
		flag.Usage()
		os.Exit(1)
	}

	// 检查文件是否存在
	if _, err := os.Stat(*csvPath); os.IsNotExist(err) {
		fmt.Printf("错误：CSV文件不存在：%s\n", *csvPath)
		os.Exit(1)
	}

	// 设置内存限制
	if err := graph.SetProcessMemLimit(*memLimitMB); err != nil {
		fmt.Printf("警告：设置内存限制失败：%v，使用系统默认\n", err)
	}

	// 初始化内存监控
	memMonitor := graph.NewMemoryMonitor(*memLimitMB)
	memMonitor.Start()
	defer memMonitor.Stop()

	// 初始化核心组件
	memPool, err := graph.NewMemoryPoolManager()
	if err != nil {
		fmt.Printf("初始化内存池失败：%v\n", err)
		os.Exit(1)
	}
	defer memPool.Close()

	csr, err := graph.NewBlockedCSR("./cold_blocks")
	if err != nil {
		fmt.Printf("初始化CSR存储失败：%v\n", err)
		os.Exit(1)
	}

	cache := graph.NewCacheManager(csr, memPool)
	loader := graph.NewCSVLoader()

	pointIDMap := graph.NewStringIdMapping()
	pointLabelMap := graph.NewLabelMapping()
	edgeLabelMap := graph.NewLabelMapping()

	// 统计行数
	lineCount, err := graph.CountCSVLines(*csvPath)
	if err != nil {
		fmt.Printf("统计CSV行数失败：%v\n", err)
		os.Exit(1)
	}
	fmt.Printf("CSV总行数：%d\n", lineCount)

	// 开始导入
	start := time.Now()
	err = loader.LoadSingleCSVConcurrent(*csvPath, pointIDMap, pointLabelMap, edgeLabelMap, csr, cache, *memLimitMB)
	if err != nil {
		fmt.Printf("导入CSV失败：%v\n", err)
		os.Exit(1)
	}

	// 释放临时内存
	cache.ReleaseImportTempMem()
	dur := time.Since(start)

	// 内存统计
	postMem := graph.GetUsedMemory()
	fmt.Printf("导入完成！耗时：%v，内存占用：%dMB，查询可用内存：%dMB\n",
		dur, postMem, *memLimitMB-postMem)

	// 初始化查询管理器
	queryMgr := graph.NewQueryManager(csr, cache, memPool)
	// 分配查询内存（预留1GB系统内存）
	queryMgr.AllocQueryMem(*memLimitMB - postMem - 1000)

	// 示例查询
	testPoint := uint32(100)
	outNeighbors, err := queryMgr.GetNeighbors(testPoint, true)
	if err != nil {
		fmt.Printf("查询出边邻居失败：%v\n", err)
	} else {
		fmt.Printf("节点%d的出边邻居数：%d\n", testPoint, len(outNeighbors))
	}

	inNeighbors, err := queryMgr.GetNeighbors(testPoint, false)
	if err != nil {
		fmt.Printf("查询入边邻居失败：%v\n", err)
	} else {
		fmt.Printf("节点%d的入边邻居数：%d\n", testPoint, len(inNeighbors))
	}
}
