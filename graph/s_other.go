package main

import (
	"runtime"
	"syscall"
	"unsafe"
)

// 常量定义
const (
	BLOCK_SIZE         = 65536                  // 每个块的节点数
	EMERGENCY_POOL_SIZE = 500 * 1024 * 1024     // 500MB应急内存池
	CACHE_LINE_SIZE    = 128                    // 缓存行大小
	BATCH_SIZE         = 100000                 // CSV解析批次大小
	HOT_BLOCK_THRESHOLD = 1000                  // 热块ID阈值
)

// 内存对齐辅助函数
func alignMemory(size uint64) uint64 {
	return (size + CACHE_LINE_SIZE - 1) & ^(CACHE_LINE_SIZE - 1)
}

// MemoryPoolManager 内存池管理
type MemoryPoolManager struct {
	emergencyPool []byte
	subgraphPool  []byte
	queryPool     []byte
}

// NewMemoryPoolManager 创建内存池
func NewMemoryPoolManager() (*MemoryPoolManager, error) {
	// 分配大页内存（mmap）
	pool, err := syscall.Mmap(-1, 0, EMERGENCY_POOL_SIZE, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS|syscall.MAP_HUGETLB)
	if err != nil {
		return nil, err
	}

	return &MemoryPoolManager{
		emergencyPool: pool,
	}, nil
}

// FreeSubgraphPool 释放子图池
func (m *MemoryPoolManager) FreeSubgraphPool() {
	if len(m.subgraphPool) > 0 {
		syscall.Munlock(m.subgraphPool)
		m.subgraphPool = nil
	}
}

// AllocQueryPool 分配查询内存池
func (m *MemoryPoolManager) AllocQueryPool(queryMemMB uint64) bool {
	totalNeed := queryMemMB * 1024 * 1024
	if totalNeed <= EMERGENCY_POOL_SIZE {
		m.queryPool = m.emergencyPool[:totalNeed]
		syscall.Mlock(m.queryPool)
		return true
	}

	extraSize := totalNeed - EMERGENCY_POOL_SIZE
	extraPool, err := syscall.Mmap(-1, 0, int(extraSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS|syscall.MAP_HUGETLB)
	if err != nil {
		m.queryPool = m.emergencyPool
		syscall.Mlock(m.queryPool)
		return false
	}

	m.queryPool = append(m.emergencyPool, extraPool...)
	syscall.Mlock(m.queryPool)
	return true
}

// Close 关闭内存池
func (m *MemoryPoolManager) Close() error {
	if err := syscall.Munmap(m.emergencyPool); err != nil {
		return err
	}
	if len(m.subgraphPool) > 0 && &m.subgraphPool[0] != &m.emergencyPool[0] {
		if err := syscall.Munmap(m.subgraphPool); err != nil {
			return err
		}
	}
	if len(m.queryPool) > 0 && &m.queryPool[0] != &m.emergencyPool[0] {
		if err := syscall.Munmap(m.queryPool); err != nil {
			return err
		}
	}
	return nil
}

// GetUsedMemory 获取当前进程内存占用（MB）
func GetUsedMemory() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / 1024 / 1024
}

// SetProcessMemLimit 设置进程内存限制（MB）
func SetProcessMemLimit(memLimitMB uint64) error {
	var rlimit syscall.Rlimit
	rlimit.Cur = memLimitMB * 1024 * 1024
	rlimit.Max = memLimitMB * 1024 * 1024
	return syscall.Setrlimit(syscall.RLIMIT_AS, &rlimit)
}