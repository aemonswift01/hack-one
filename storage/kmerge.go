package storage

import (
	"bytes"
	"unsafe"
)

type loserTree struct {
	size  int
	nodes []int
	iters []*nodeFileIter
	err   error
}

func newLoserTree(data []nodesFile) *loserTree {
	leafSize := len(data)

	iters := make([]*nodeFileIter, 0, len(data))
	for _, nf := range data {
		iters = append(iters, newNodeFileIter(nf))
	}

	lt := &loserTree{
		size:  leafSize,
		iters: iters,
		nodes: make([]int, leafSize),
	}

	for i := range lt.nodes {
		lt.nodes[i] = -1
	}

	for i := range lt.nodes {
		lt.adjust(i)
	}

	return lt
}

func (l *loserTree) adjust(i int) {
	t := (l.size + i) >> 1
	for t > 0 {
		if l.nodes[t] == -1 {
			l.nodes[t] = i
			i = t
			break
		}

		loser := l.nodes[t]
		if l.iters[i].empty() ||
			(!l.iters[loser].empty() && l.compare(loser, i)) {
			l.nodes[t] = i
			i = loser
		}
		t = t >> 1
	}
	l.nodes[0] = i
}

func (l *loserTree) compare(i, j int) bool {
	bi, err := l.iters[i].next()
	if err != nil {
		panic(err)
	}
	idi := bi[8 : 8+*(*int32)(unsafe.Pointer(&bi[0]))]
	bj, err := l.iters[j].next()
	if err != nil {
		panic(err)
	}
	idj := bj[8 : 8+*(*int32)(unsafe.Pointer(&bj[0]))]

	return bytes.Compare(idi, idj) < 0
}

func (l *loserTree) pop() []byte {
	index := l.nodes[0]
	obj := l.iters[index].pick()
	l.adjust(index)
	return obj
}
