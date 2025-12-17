package storage

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"sort"
	"strconv"
	"unsafe"

	"golang.org/x/sys/unix"
)

const chunkSize = 1 * 1024 * 1024 * 1024 // 1GB

var (
	cvsSplit string = ","

	errChunkFull = errors.New("chunk is full")
)

type Storage struct {
	nextFileID uint32
	BaseDir    string
}

func (s *Storage) BuildFromCSV(baseDir, csvPath string) error {
	f, err := os.OpenFile(csvPath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	var (
		br               = bufio.NewReader(f)
		lcache           = make([]byte, 0, 1024)
		memChunk         = s.memChunk(chunkSize)
		offsets          = make([]uint64, 0, 1024)
		chunkMaxNodeSize = 0
		chunkNodeCount   = 0
		totalNodeCount   = 0
		totalMaxNodeSize = 0

		nodesChkFiles = make([]nodesFile, 0, 1)
	)

	writeNode := func(chunk chunk, id []byte, label []byte) error {
		if chunk.offset+uint64(8+len(id)+len(label)) > uint64(len(chunk.data)) {
			return errChunkFull
		}

		idLen := uint32(len(id))
		labelLen := uint32(len(label))
		n := copy(memChunk.data[memChunk.offset:], (*(*[4]byte)(unsafe.Pointer(&idLen)))[:])
		n += copy(memChunk.data[memChunk.offset+uint64(n):], (*(*[4]byte)(unsafe.Pointer(&labelLen)))[:])
		n += copy(memChunk.data[memChunk.offset+uint64(n):], id)
		n += copy(memChunk.data[memChunk.offset+uint64(n):], label)
		memChunk.offset += uint64(n)
		return nil
	}

	for {
		line, isPrefix, err := br.ReadLine()
		if err != nil {
			break
		}
		lcache = append(lcache, line...)
		if isPrefix {
			continue
		}

		ldata := bytes.Split(lcache, *(*[]byte)(unsafe.Pointer(&cvsSplit)))

		_ = ldata[4]

		offsets = append(offsets, memChunk.offset)

		if err = writeNode(memChunk, ldata[0], ldata[1]); err != nil {
			var nodeFile nodesFile
			s.sort(memChunk, offsets)
			nodeFile, memChunk, err = s.flushMemChunk(chunkNodeCount, chunkMaxNodeSize, offsets, memChunk, "node_chunk_")
			if err != nil {
				return err
			}
			nodesChkFiles = append(nodesChkFiles, nodeFile)

			chunkNodeCount = 0
			chunkMaxNodeSize = 0

			if err = writeNode(memChunk, ldata[0], ldata[1]); err != nil {
				panic("the size of node is more than a chunk")
			}
		}
		chunkNodeCount++
		totalNodeCount++

		// write dst node
		offsets = append(offsets, memChunk.offset)
		if err = writeNode(memChunk, ldata[3], ldata[4]); err != nil {
			var nodeFile nodesFile
			s.sort(memChunk, offsets)
			nodeFile, memChunk, err = s.flushMemChunk(chunkNodeCount, chunkMaxNodeSize, offsets, memChunk, "node_chunk_")
			if err != nil {
				return err
			}
			nodesChkFiles = append(nodesChkFiles, nodeFile)

			chunkNodeCount = 0
			chunkMaxNodeSize = 0

			if err = writeNode(memChunk, ldata[0], ldata[1]); err != nil {
				panic("the size of node is more than a chunk")
			}
		}
		chunkNodeCount++
		totalNodeCount++

		ml := len(ldata[0]) + len(ldata[1]) + 8
		el := len(ldata[3]) + len(ldata[4]) + 8
		if ml < el {
			ml = el
			if ml > chunkMaxNodeSize {
				chunkMaxNodeSize = ml
			}
			if ml > totalMaxNodeSize {
				totalMaxNodeSize = ml
			}
		}

		// TODO: write edges

		lcache = lcache[:0]
	}

	allNode, err := s.nextFile("node")
	if err != nil {
		return err
	}
	allNodeChk, err := s.mmapChunk(allNode, totalNodeCount*totalMaxNodeSize)
	if err != nil {
		return err
	}

	lt := newLoserTree(nodesChkFiles)
	var last []byte
	for {
		node := lt.pop()
		if node == nil {
			break
		}

		if bytes.Equal(last, node) {
			continue
		}
		last = append(last[:0], node...)

		_ = copy(allNodeChk.data[allNodeChk.offset:], node)
		allNodeChk.offset += uint64(totalMaxNodeSize)
	}

	return nil
}

type chunk struct {
	data   []byte
	offset uint64
}

func (s *Storage) memChunk(chunkSize int) chunk {
	return chunk{
		data:   make([]byte, chunkSize),
		offset: 0,
	}
}

func (s *Storage) mmapChunk(f *os.File, size int) (chunk, error) {
	data, err := unix.Mmap(
		int(f.Fd()),
		0,
		size,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if err != nil {
		return chunk{}, err
	}

	return chunk{
		data:   data,
		offset: 0,
	}, nil
}

func (s *Storage) unmapChunk(c chunk) error {
	return unix.Munmap(c.data)
}

func (s *Storage) sort(c chunk, offsets []uint64) error {
	sort.Slice(offsets, func(i, j int) bool {
		offI := offsets[i]
		offJ := offsets[j]

		lenI := *(*uint32)(unsafe.Pointer(&c.data[offI]))
		lenJ := *(*uint32)(unsafe.Pointer(&c.data[offJ]))

		idI := c.data[offI+8 : offI+8+uint64(lenI)]
		idJ := c.data[offJ+8 : offJ+8+uint64(lenJ)]

		return bytes.Compare(idI, idJ) < 0
	})
	return nil
}

type nodesFile struct {
	file      *os.File
	nodeSize  int
	nodeCount int
}

func (s *Storage) flushMemChunk(
	nc int, ns int,
	offsets []uint64,
	memChunk chunk, name string,
) (nodesFile, chunk, error) {
	size := nc*ns + 4
	f, err := s.nextFile(name)
	if err != nil {
		return nodesFile{}, chunk{}, err
	}
	defer f.Close()

	c, err := s.mmapChunk(f, size)
	if err != nil {
		return nodesFile{}, chunk{}, err
	}

	for _, offset := range offsets {
		idLen := *(*uint32)(unsafe.Pointer(&memChunk.data[offset]))
		labelLen := *(*uint32)(unsafe.Pointer(&memChunk.data[offset+4]))

		copy(c.data[c.offset:], memChunk.data[offset:offset+8+uint64(idLen)+uint64(labelLen)])
		c.offset += uint64(ns)
	}
	if err = s.unmapChunk(c); err != nil {
		return nodesFile{}, chunk{}, err
	}

	memChunk.offset = 0
	f.Seek(0, 0)
	return nodesFile{
		file:      f,
		nodeSize:  ns,
		nodeCount: nc,
	}, memChunk, nil
}

func (s *Storage) nextFile(name string) (*os.File, error) {
	id := strconv.Itoa(int(s.nextFileID))
	return os.OpenFile(name+id, os.O_CREATE|os.O_WRONLY, 0644)
}
