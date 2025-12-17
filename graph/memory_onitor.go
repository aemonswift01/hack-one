package main

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
)

// MemoryMonitor 内存监控器
type MemoryMonitor struct {
	memLimitMB     uint64
	currentRSS     uint64
	memUsageRatio  float64
	isRunning      atomic.Bool
}

// NewMemoryMonitor 创建内存监控器
func NewMemoryMonitor(memLimitMB uint64) *MemoryMonitor {
	return &MemoryMonitor{
		memLimitMB: memLimitMB,
	}
}

// Start 启动监控
func (m *MemoryMonitor) Start() {
	m.isRunning.Store(true)
	go func() {
		for m.isRunning.Load() {
			// 采样内存
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			m.currentRSS = stats.Alloc / 1024 / 1024
			m.memUsageRatio = float64(m.currentRSS) / float64(m.memLimitMB)

			// 打印监控日志
			m.LogMemStatus()

			time.Sleep(1 * time.Second)
		}
	}()
}

// Stop 停止监控
func (m *MemoryMonitor) Stop() {
	m.isRunning.Store(false)
}

// CheckThreshold 检查内存阈值
// return: true=触发硬限制（需停止操作），false=仅预警
func (m *MemoryMonitor) CheckThreshold(warnRatio, limitRatio float64) bool {
	ratio := m.memUsageRatio
	if ratio >= limitRatio {
		fmt.Printf("ERROR: 内存占用达到硬限制 %.2f%%，停止导入\n", ratio*100)
		return true
	} else if ratio >= warnRatio {
		fmt.Printf("WARNING: 内存占用过高 %.2f%%，降低导入速度\n", ratio*100)
		return false
	}
	return false
}

// LogMemStatus 打印内存状态
func (m *MemoryMonitor) LogMemStatus() {
	fmt.Printf("[内存监控] 已用: %dMB / 限制: %dMB (%.2f%%)\n",
		m.currentRSS, m.memLimitMB, m.memUsageRatio*100)
}

// GetCurrentRSS 获取当前内存占用（MB）
func (m *MemoryMonitor) GetCurrentRSS() uint64 {
	return m.currentRSS
}

// GetMemUsageRatio 获取内存占用比例
func (m *MemoryMonitor) GetMemUsageRatio() float64 {
	return m.memUsageRatio
}