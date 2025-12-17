package main

// Iterator 基础迭代器接口
type Iterator interface {
	HasNext() bool
	Next()
	Reset()
}

// AdjIterator 邻接迭代器接口
type AdjIterator interface {
	Iterator
	GetValue() (edgeID uint32, nodeID uint32) // 返回边ID和节点ID
}