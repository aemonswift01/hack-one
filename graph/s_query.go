package graph

// AdjQueryIterator 邻接查询迭代器
type AdjQueryIterator struct {
	block      *Block
	startIdx   uint32
	endIdx     uint32
	currentIdx uint32
	isOut      bool
}

// NewAdjQueryIterator 创建邻接迭代器
func NewAdjQueryIterator(block *Block, localID uint32, isOut bool) *AdjQueryIterator {
	var startIdx, endIdx uint32
	if isOut {
		startIdx = block.OffsetsOut[localID]
		endIdx = block.OffsetsOut[localID+1]
	} else {
		startIdx = block.OffsetsIn[localID]
		endIdx = block.OffsetsIn[localID+1]
	}

	return &AdjQueryIterator{
		block:      block,
		startIdx:   startIdx,
		endIdx:     endIdx,
		currentIdx: startIdx,
		isOut:      isOut,
	}
}

// HasNext 是否有下一个
func (a *AdjQueryIterator) HasNext() bool {
	return a.currentIdx < a.endIdx
}

// Next 下一个
func (a *AdjQueryIterator) Next() {
	a.currentIdx++
}

// Reset 重置
func (a *AdjQueryIterator) Reset() {
	a.currentIdx = a.startIdx
}

// GetValue 获取值（EdgeID, NodeID）
func (a *AdjQueryIterator) GetValue() (uint32, uint32) {
	edge := a.block.AdjData[a.currentIdx]
	return edge.EdgeID, edge.NodeID
}

// QueryManager 查询管理器
type QueryManager struct {
	csr     *BlockedCSR
	cache   *CacheManager
	memPool *MemoryPoolManager
}

// NewQueryManager 创建查询管理器
func NewQueryManager(csr *BlockedCSR, cache *CacheManager, memPool *MemoryPoolManager) *QueryManager {
	return &QueryManager{
		csr:     csr,
		cache:   cache,
		memPool: memPool,
	}
}

// GetAdjIterator 获取邻接迭代器
func (q *QueryManager) GetAdjIterator(pointID uint32, isOut bool) (*AdjQueryIterator, error) {
	blockID := pointID / BLOCK_SIZE
	localID := pointID % BLOCK_SIZE
	block, err := q.cache.GetBlock(blockID)
	if err != nil {
		return nil, err
	}
	return NewAdjQueryIterator(block, localID, isOut), nil
}

// GetNeighbors 获取邻居节点
func (q *QueryManager) GetNeighbors(pointID uint32, isOut bool) ([]uint32, error) {
	iter, err := q.GetAdjIterator(pointID, isOut)
	if err != nil {
		return nil, err
	}

	neighbors := make([]uint32, 0)
	for iter.HasNext() {
		_, nodeID := iter.GetValue()
		neighbors = append(neighbors, nodeID)
		iter.Next()
	}
	return neighbors, nil
}

// AllocQueryMem 分配查询内存
func (q *QueryManager) AllocQueryMem(memMB uint64) bool {
	return q.memPool.AllocQueryPool(memMB)
}
