package graph

import (
	"bufio"
	"compress/lzw"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

// EdgeData 边数据结构（出边+入边）
type EdgeData struct {
	EdgeID       uint32 // 8亿边ID（32bit）
	NodeID       uint32 // 3亿点ID（32bit）
	EdgeLabelID  uint8  // 边标签ID（0-255）
	IsOut        uint8  // 0=入边，1=出边
	StartLabelID uint8  // 起点标签ID
	EndLabelID   uint8  // 终点标签ID
}

// Block 块结构（CSR核心）
type Block struct {
	BlockID    uint32
	OffsetsOut []uint32 // 出边偏移
	OffsetsIn  []uint32 // 入边偏移
	AdjData    []EdgeData
}

// NewBlock 创建新块
func NewBlock(blockID uint32) *Block {
	return &Block{
		BlockID:    blockID,
		OffsetsOut: make([]uint32, BLOCK_SIZE+1),
		OffsetsIn:  make([]uint32, BLOCK_SIZE+1),
		AdjData:    make([]EdgeData, 0),
	}
}

// BlockedCSR CSR存储管理
type BlockedCSR struct {
	coldBlockDir string
}

// NewBlockedCSR 创建CSR存储
func NewBlockedCSR(dir string) (*BlockedCSR, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &BlockedCSR{
		coldBlockDir: dir,
	}, nil
}

// getBlockPath 获取块文件路径
func (c *BlockedCSR) getBlockPath(blockID uint32) string {
	return filepath.Join(c.coldBlockDir, fmt.Sprintf("block_%d.dat", blockID))
}

// SaveBlockToDisk 保存块到磁盘（LZW压缩+mmap）
func (c *BlockedCSR) SaveBlockToDisk(block *Block) error {
	path := c.getBlockPath(block.BlockID)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// 计算数据大小
	adjSize := len(block.AdjData) * int(unsafe.Sizeof(EdgeData{}))
	offsetsOutSize := len(block.OffsetsOut) * int(unsafe.Sizeof(uint32(0)))
	offsetsInSize := len(block.OffsetsIn) * int(unsafe.Sizeof(uint32(0)))
	totalSize := adjSize + offsetsOutSize + offsetsInSize + 4 // +4 for blockID

	// 预分配文件大小
	if err := f.Truncate(int64(totalSize)); err != nil {
		return err
	}

	// mmap映射文件
	data, err := syscall.Mmap(int(f.Fd()), 0, totalSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	defer syscall.Munmap(data)

	// 写入数据
	ptr := 0
	// 写入BlockID
	*(*uint32)(unsafe.Pointer(&data[ptr])) = block.BlockID
	ptr += 4

	// 写入OffsetsOut
	copy(data[ptr:ptr+offsetsOutSize], unsafe.Slice((*byte)(unsafe.Pointer(&block.OffsetsOut[0])), offsetsOutSize))
	ptr += offsetsOutSize

	// 写入OffsetsIn
	copy(data[ptr:ptr+offsetsInSize], unsafe.Slice((*byte)(unsafe.Pointer(&block.OffsetsIn[0])), offsetsInSize))
	ptr += offsetsInSize

	// 压缩并写入AdjData
	writer := lzw.NewWriter(bufio.NewWriter(unsafe.Slice((*byte)(unsafe.Pointer(&data[ptr])), adjSize)), lzw.LSB, 8)
	defer writer.Close()
	_, err = writer.Write(unsafe.Slice((*byte)(unsafe.Pointer(&block.AdjData[0])), adjSize))
	if err != nil {
		return err
	}

	// 刷盘
	return syscall.Msync(data, syscall.MS_SYNC)
}

// LoadColdBlock 加载冷块（mmap+解压）
func (c *BlockedCSR) LoadColdBlock(blockID uint32) (*Block, error) {
	path := c.getBlockPath(blockID)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// 获取文件大小
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()

	// mmap映射
	data, err := syscall.Mmap(int(f.Fd()), 0, int(fileSize), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	defer syscall.Munmap(data)

	// 解析数据
	ptr := 0
	blockIDFromFile := *(*uint32)(unsafe.Pointer(&data[ptr]))
	ptr += 4

	block := NewBlock(blockIDFromFile)
	offsetsOutSize := (BLOCK_SIZE + 1) * int(unsafe.Sizeof(uint32(0)))
	copy(unsafe.Slice((*byte)(unsafe.Pointer(&block.OffsetsOut[0])), offsetsOutSize), data[ptr:ptr+offsetsOutSize])
	ptr += offsetsOutSize

	offsetsInSize := (BLOCK_SIZE + 1) * int(unsafe.Sizeof(uint32(0)))
	copy(unsafe.Slice((*byte)(unsafe.Pointer(&block.OffsetsIn[0])), offsetsInSize), data[ptr:ptr+offsetsInSize])
	ptr += offsetsInSize

	// 解压AdjData
	reader := lzw.NewReader(bufio.NewReader(unsafe.Slice((*byte)(unsafe.Pointer(&data[ptr])), int(fileSize)-ptr)), lzw.LSB, 8)
	defer reader.Close()
	adjData := make([]EdgeData, 0)
	buf := make([]byte, int(unsafe.Sizeof(EdgeData{})))
	for {
		n, err := reader.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if n == int(unsafe.Sizeof(EdgeData{})) {
			adjData = append(adjData, *(*EdgeData)(unsafe.Pointer(&buf[0])))
		}
	}
	block.AdjData = adjData

	return block, nil
}

// AddBlock 添加块（热块/冷块）
func (c *BlockedCSR) AddBlock(block *Block, isHot bool) error {
	if !isHot {
		return c.SaveBlockToDisk(block)
	}
	return nil // 热块由CacheManager管理
}
