package storage

import "errors"

var ErrIterEnd = errors.New("Iterator reached the end")

type nodeFileIter struct {
	buf       []byte
	nodesFile nodesFile
	n         int
}

func newNodeFileIter(nf nodesFile) *nodeFileIter {
	return &nodeFileIter{
		buf:       make([]byte, nf.nodeSize),
		nodesFile: nf,
		n:         0,
	}
}

func (nfi *nodeFileIter) empty() bool {
	return nfi.n > nfi.nodesFile.nodeCount
}

func (nfi *nodeFileIter) pick() []byte {
	return nfi.buf
}

func (nfi *nodeFileIter) next() ([]byte, error) {
	if nfi.n >= nfi.nodesFile.nodeCount {
		nfi.buf = nil
		nfi.n++
		return nil, ErrIterEnd
	}

	_, err := nfi.nodesFile.file.Read(nfi.buf)
	if err != nil {
		return nil, err
	}
	nfi.n++

	return nfi.buf, nil
}
