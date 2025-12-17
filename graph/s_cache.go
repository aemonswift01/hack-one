package graph

import (
	"runtime"
	"sync"
	"unsafe"
)

// CacheManager 缓存管理
type CacheManager struct {
	csr           *BlockedCSR
	memPool       *MemoryPoolManager
	hotBlocks     sync.Map
	maxHotMem     uint64
	currentHotMem uint64
	mu            sync.Mutex
}

// NewCacheManager 创建缓存管理器
func NewCacheManager(csr *BlockedCSR, memPool *MemoryPoolManager) *CacheManager {
	return &CacheManager{
		csr:       csr,
		memPool:   memPool,
		maxHotMem: 1 * 1024 * 1024 * 1024, // 1GB热块缓存
	}
}

// calcBlockMem 计算块内存占用
func (c *CacheManager) calcBlockMem(block *Block) uint64 {
	return uint64(len(block.AdjData))*uint64(unsafe.Sizeof(EdgeData{})) +
		uint64(len(block.OffsetsOut))*uint64(unsafe.Sizeof(uint32(0))) +
		uint64(len(block.OffsetsIn))*uint64(unsafe.Sizeof(uint32(0)))
}

// EvictColdBlock 驱逐冷块
func (c *CacheManager) EvictColdBlock() {
	c.mu.Lock()
	defer c.mu.Unlock()

	var evictID uint32
	c.hotBlocks.Range(func(key, value any) bool {
		evictID = key.(uint32)
		return false
	})
	if evictID == 0 {
		return
	}
	block, ok := c.hotBlocks.Load(evictID)
	if !ok {
		return
	}
	c.currentHotMem -= c.calcBlockMem(block.(*Block))
	c.hotBlocks.Delete(evictID)
}

// GetBlock 获取块（优先热块）
func (c *CacheManager) GetBlock(blockID uint32) (*Block, error) {
	// 检查热块
	if block, ok := c.hotBlocks.Load(blockID); ok {
		return block.(*Block), nil
	}

	// 加载冷块
	block, err := c.csr.LoadColdBlock(blockID)
	if err != nil {
		block = NewBlock(blockID)
	}

	// 加入热块
	c.mu.Lock()
	mem := c.calcBlockMem(block)
	c.currentHotMem += mem
	for c.currentHotMem > c.maxHotMem {
		c.EvictColdBlock()
	}
	c.hotBlocks.Store(blockID, block)
	c.mu.Unlock()

	return block, nil
}

// AddHotBlock 添加热块
func (c *CacheManager) AddHotBlock(block *Block) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	mem := c.calcBlockMem(block)
	for c.currentHotMem+mem > c.maxHotMem {
		c.EvictColdBlock()
	}
	c.hotBlocks.Store(block.BlockID, block)
	c.currentHotMem += mem
	return nil
}

// ShrinkHotCache 收缩热缓存
func (c *CacheManager) ShrinkHotCache(ratio float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	target := uint64(float64(c.maxHotMem) * ratio)
	for c.currentHotMem > target {
		c.EvictColdBlock()
	}
}

// ReleaseImportTempMem 释放导入临时内存
func (c *CacheManager) ReleaseImportTempMem() {
	c.hotBlocks.Range(func(key, value any) bool {
		blockID := key.(uint32)
		if blockID >= HOT_BLOCK_THRESHOLD {
			block := value.(*Block)
			block.AdjData = nil
			block.AdjData = make([]EdgeData, 0)
		}
		return true
	})

	// 重置内存统计
	c.currentHotMem = 0
	c.hotBlocks.Range(func(key, value any) bool {
		c.currentHotMem += c.calcBlockMem(value.(*Block))
		return true
	})

	// 强制内存回收
	runtime.GC()
}

// GetCurrentHotMem 获取当前热块内存占用
func (c *CacheManager) GetCurrentHotMem() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentHotMem
}
