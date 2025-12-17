package storage

import (
	"bytes"
	"unsafe"
)

type mergeTree struct {
	size  int
	nodes []int
	iters []*nodeFileIter
}

func newMergeTree(data []nodesFile) (*mergeTree, error) {
	leafSize := len(data)

	iters := make([]*nodeFileIter, 0, len(data))
	for _, nf := range data {
		nfi := newNodeFileIter(nf)
		if _, err := nfi.next(); err != nil {
			return nil, err
		}
		iters = append(iters, nfi)
	}

	lt := &mergeTree{
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

	return lt, nil
}

func (l *mergeTree) adjust(i int) {
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

func (l *mergeTree) compare(i, j int) bool {
	bi := l.iters[i].pick()
	idi := bi[8 : 8+*(*uint32)(unsafe.Pointer(&bi[0]))]

	bj := l.iters[j].pick()
	idj := bj[8 : 8+*(*uint32)(unsafe.Pointer(&bj[0]))]
	return bytes.Compare(idi, idj) < 0
}

func (l *mergeTree) pop() []byte {
	index := l.nodes[0]
	return l.iters[index].pick()
}

func (l *mergeTree) popAdjust() error {
	index := l.nodes[0]
	_, err := l.iters[index].next()
	if err != nil && err != ErrIterEnd {
		return err
	}

	l.adjust(index)
	return nil
}
