package main

import (
	"bufio"
	"os"
	"strings"
	"sync"
)

// LabelMapping 标签映射
type LabelMapping struct {
	strToID map[string]uint8
	idToStr map[uint8]string
	mu      sync.Mutex
	nextID  uint8
}

// NewLabelMapping 创建标签映射
func NewLabelMapping() *LabelMapping {
	return &LabelMapping{
		strToID: make(map[string]uint8),
		idToStr: make(map[uint8]string),
		nextID:  1, // 0为无效ID
	}
}

// GetID 获取标签ID
func (l *LabelMapping) GetID(str string) uint8 {
	l.mu.Lock()
	defer l.mu.Unlock()

	if id, ok := l.strToID[str]; ok {
		return id
	}
	id := l.nextID
	l.nextID++
	l.strToID[str] = id
	l.idToStr[id] = str
	return id
}

// StringIdMapping 字符串ID映射
type StringIdMapping struct {
	strToID map[string]uint32
	idToStr map[uint32]string
	mu      sync.Mutex
	nextID  uint32
}

// NewStringIdMapping 创建字符串ID映射
func NewStringIdMapping() *StringIdMapping {
	return &StringIdMapping{
		strToID: make(map[string]uint32),
		idToStr: make(map[uint32]string),
		nextID:  0,
	}
}

// GetIntID 获取整数ID
func (s *StringIdMapping) GetIntID(str string) uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if id, ok := s.strToID[str]; ok {
		return id
	}
	id := s.nextID
	s.nextID++
	s.strToID[str] = id
	s.idToStr[id] = str
	return id
}

// CSVLoader CSV加载器
type CSVLoader struct {
	nextEdgeID uint32
	mu         sync.Mutex
}

// NewCSVLoader 创建CSV加载器
func NewCSVLoader() *CSVLoader {
	return &CSVLoader{
		nextEdgeID: 0,
	}
}

// getEdgeID 获取边ID
func (c *CSVLoader) getEdgeID() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextEdgeID
	c.nextEdgeID++
	return id
}

// parseCSVRecord 解析单条CSV记录
func (c *CSVLoader) parseCSVRecord(line string, pointIDMap *StringIdMapping,
	pointLabelMap *LabelMapping, edgeLabelMap *LabelMapping,
	blockBuilders *sync.Map) {

	fields := strings.Split(line, ",")
	if len(fields) < 5 {
		return
	}

	startStr := fields[0]
	endStr := fields[1]
	edgeLabel := fields[2]
	startLabel := fields[3]
	endLabel := fields[4]

	// 获取ID
	startID := pointIDMap.GetIntID(startStr)
	endID := pointIDMap.GetIntID(endStr)
	edgeLabelID := edgeLabelMap.GetID(edgeLabel)
	startLabelID := pointLabelMap.GetID(startLabel)
	endLabelID := pointLabelMap.GetID(endLabel)
	edgeID := c.getEdgeID()

	// 计算块ID
	startBlockID := startID / BLOCK_SIZE
	endBlockID := endID / BLOCK_SIZE
	startLocalID := startID % BLOCK_SIZE
	endLocalID := endID % BLOCK_SIZE

	// 构建出边
	outEdge := EdgeData{
		EdgeID:        edgeID,
		NodeID:        endID,
		EdgeLabelID:   edgeLabelID,
		IsOut:         1,
		StartLabelID:  startLabelID,
		EndLabelID:    endLabelID,
	}

	// 构建入边
	inEdge := EdgeData{
		EdgeID:        edgeID,
		NodeID:        startID,
		EdgeLabelID:   edgeLabelID,
		IsOut:         0,
		StartLabelID:  endLabelID,
		EndLabelID:    startLabelID,
	}

	// 写入出边块
	startBlockVal, _ := blockBuilders.LoadOrStore(startBlockID, NewBlock(startBlockID))
	startBlock := startBlockVal.(*Block)
	startBlock.AdjData = append(startBlock.AdjData, outEdge)
	startBlock.OffsetsOut[startLocalID+1]++

	// 写入入边块
	endBlockVal, _ := blockBuilders.LoadOrStore(endBlockID, NewBlock(endBlockID))
	endBlock := endBlockVal.(*Block)
	endBlock.AdjData = append(endBlock.AdjData, inEdge)
	endBlock.OffsetsIn[endLocalID+1]++
}

// parseCSVRecordBatch 解析CSV批次
func (c *CSVLoader) parseCSVRecordBatch(lines []string, pointIDMap *StringIdMapping,
	pointLabelMap *LabelMapping, edgeLabelMap *LabelMapping,
	blockBuilders *sync.Map) {

	for _, line := range lines {
		c.parseCSVRecord(line, pointIDMap, pointLabelMap, edgeLabelMap, blockBuilders)
	}
}

// LoadSingleCSVConcurrent 并发加载CSV
func (c *CSVLoader) LoadSingleCSVConcurrent(csvPath string, pointIDMap *StringIdMapping,
	pointLabelMap *LabelMapping, edgeLabelMap *LabelMapping,
	csr *BlockedCSR, cache *CacheManager, memLimitMB uint64) error {

	f, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// 跳过表头
	scanner.Scan()

	var wg sync.WaitGroup
	batchLines := make([]string, 0, BATCH_SIZE)
	blockBuilders := &sync.Map{}
	lineCount := 0

	for scanner.Scan() {
		line := scanner.Text()
		batchLines = append(batchLines, line)
		lineCount++

		if len(batchLines) >= BATCH_SIZE {
			// 检查内存
			currentMem := GetUsedMemory()
			if currentMem >= uint64(float64(memLimitMB)*0.9) {
				// 回收内存
				blockBuilders.Range(func(key, value any) bool {
					block := value.(*Block)
					csr.AddBlock(block, false)
					block.AdjData = nil
					block.AdjData = make([]EdgeData, 0)
					return true
				})
				cache.ShrinkHotCache(0.5)
				runtime.GC()
			}

			// 并发解析
			batch := make([]string, len(batchLines))
			copy(batch, batchLines)
			wg.Add(1)
			go func() {
				defer wg.Done()
				c.parseCSVRecordBatch(batch, pointIDMap, pointLabelMap, edgeLabelMap, blockBuilders)
			}()
			batchLines = batchLines[:0]
		}
	}

	// 处理剩余行
	if len(batchLines) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.parseCSVRecordBatch(batchLines, pointIDMap, pointLabelMap, edgeLabelMap, blockBuilders)
		}()
	}
	wg.Wait()

	// 合并块
	mergedBlocks := make(map[uint32]*Block)
	blockBuilders.Range(func(key, value any) bool {
		blockID := key.(uint32)
		block := value.(*Block)
		if mergedBlock, ok := mergedBlocks[blockID]; ok {
			mergedBlock.AdjData = append(mergedBlock.AdjData, block.AdjData...)
			for i := range mergedBlock.OffsetsOut {
				mergedBlock.OffsetsOut[i] += block.OffsetsOut[i]
				mergedBlock.OffsetsIn[i] += block.OffsetsIn[i]
			}
		} else {
			mergedBlocks[blockID] = block
		}
		return true
	})

	// 写入块
	for blockID, block := range mergedBlocks {
		if blockID < HOT_BLOCK_THRESHOLD {
			cache.AddHotBlock(block)
		} else {
			csr.AddBlock(block, false)
		}
	}

	return nil
}

// CountCSVLines 统计CSV行数
func CountCSVLines(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	count := uint64(0)
	for scanner.Scan() {
		count++
	}
	return count - 1, scanner.Err() // 减表头
}