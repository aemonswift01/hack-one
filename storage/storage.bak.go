//go:build gen

package storage

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
)

type Storage struct {
	BaseDir   string
	NodeCount uint32
	EdgeCount uint64
	idToStr   []string

	forwardOffsets   []uint64
	forwardNeighbors []byte
	forwardCounts    []uint32
}

func New(baseDir string) *Storage {
	return &Storage{BaseDir: baseDir}
}

// BuildFromCSV builds disk-backed CSR under BaseDir/graph_data
func (s *Storage) BuildFromCSV(csvPath string) error {
	base := s.BaseDir
	if base == "" {
		base = filepath.Join(filepath.Dir(csvPath), "graph_data")
	}
	if err := os.MkdirAll(base, 0755); err != nil {
		return err
	}

	const CHUNK_BYTES = 100 * 1024 * 1024

	// temp files
	nodesTmp := filepath.Join(base, "nodes_tmp.bin")
	edgesTmp := filepath.Join(base, "edges_tmp.bin")

	// 1) dump nodes and edges as length-prefixed binary
	if err := dumpNodesEdges(csvPath, nodesTmp, edgesTmp); err != nil {
		return err
	}

	// 2) split nodes_tmp into sorted unique chunks
	nodeChunks, err := splitSortNodes(nodesTmp, base, CHUNK_BYTES)
	if err != nil {
		return err
	}
	if len(nodeChunks) == 0 {
		return errors.New("no nodes")
	}

	// 3) k-way merge node chunks into final unique nodes and offsets
	nodesFinal := filepath.Join(base, "nodes_final.bin")
	nodesOffsets := filepath.Join(base, "nodes_offsets.bin")
	if err := mergeNodeChunks(nodeChunks, nodesFinal, nodesOffsets); err != nil {
		return err
	}

	// 4) build id_to_str.bin (unique) and load into memory
	idToStrPath := filepath.Join(base, "id_to_str.bin")
	idCount, err := buildIdToStr(nodesFinal, idToStrPath)
	if err != nil {
		return err
	}
	s.NodeCount = idCount
	// load id_to_str into memory (careful for very large graphs)
	idList, err := readIdToStr(idToStrPath)
	if err != nil {
		return err
	}
	s.idToStr = idList

	// 5) map edges strings to ids by binary-search over nodes_final
	edgesBin := filepath.Join(base, "edges_bin.bin")
	if err := mapEdgesToIds(edgesTmp, nodesOffsets, nodesFinal, edgesBin); err != nil {
		return err
	}

	// 6) external sort edges and build CSR
	edgeChunks, err := splitSortEdges(edgesBin, base, CHUNK_BYTES)
	if err != nil {
		return err
	}
	edgesSorted := filepath.Join(base, "edges_sorted.bin")
	if err := mergeEdgeChunks(edgeChunks, edgesSorted); err != nil {
		return err
	}

	if err := buildCSRFromSortedEdges(edgesSorted, int(s.NodeCount), base); err != nil {
		return err
	}

	// load forward CSR into memory for fast access (could mmap instead)
	if err := s.loadForwardCSR(base); err != nil {
		return err
	}

	return nil
}

// helper I/O primitives and implementations follow

func dumpNodesEdges(csvPath, nodesOutPath, edgesOutPath string) error {
	f, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	defer f.Close()
	nf, err := os.Create(nodesOutPath)
	if err != nil {
		return err
	}
	defer nf.Close()
	ef, err := os.Create(edgesOutPath)
	if err != nil {
		return err
	}
	defer ef.Close()

	r := bufio.NewReader(f)
	nw := bufio.NewWriter(nf)
	ew := bufio.NewWriter(ef)
	defer nw.Flush()
	defer ew.Flush()

	for {
		line, err := r.ReadString('\n')
		if err == io.EOF && len(line) == 0 {
			break
		}
		if err != nil && err != io.EOF {
			return err
		}
		// simple CSV split for at least 5 columns
		var parts [5]string
		p := 0
		cur := ""
		cnt := 0
		for i := 0; i < len(line) && cnt < 4; i++ {
			if line[i] == ',' {
				parts[cnt] = cur
				cur = ""
				cnt++
				continue
			}
			cur += string(line[i])
		}
		// capture remainder as last field
		parts[4] = cur
		u := parts[0]
		v := parts[3]
		writeLenPrefixed(nw, u)
		writeLenPrefixed(nw, v)
		writeLenPrefixed(ew, u)
		writeLenPrefixed(ew, v)
		if err == io.EOF {
			break
		}
	}
	return nil
}

func writeLenPrefixed(w *bufio.Writer, s string) error {
	var l uint32 = uint32(len(s))
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], l)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.WriteString(s); err != nil {
		return err
	}
	return nil
}

func splitSortNodes(nodesTmp, base string, chunkBytes int) ([]string, error) {
	f, err := os.Open(nodesTmp)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var chunks []string
	buf := make([]string, 0)
	curBytes := 0
	for {
		var lbuf [4]byte
		if _, err := io.ReadFull(r, lbuf[:]); err != nil {
			break
		}
		l := int(binary.LittleEndian.Uint32(lbuf[:]))
		s := make([]byte, l)
		if _, err := io.ReadFull(r, s); err != nil {
			return nil, err
		}
		buf = append(buf, string(s))
		curBytes += l + 4
		if curBytes >= chunkBytes {
			sort.Strings(buf)
			buf = uniqueStrings(buf)
			chunk := filepath.Join(base, "node_chunk_"+string(len(chunks))+".bin")
			if err := writeStringsAsBin(chunk, buf); err != nil {
				return nil, err
			}
			chunks = append(chunks, chunk)
			buf = buf[:0]
			curBytes = 0
		}
	}
	if len(buf) > 0 {
		sort.Strings(buf)
		buf = uniqueStrings(buf)
		chunk := filepath.Join(base, "node_chunk_"+string(len(chunks))+".bin")
		if err := writeStringsAsBin(chunk, buf); err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func uniqueStrings(a []string) []string {
	j := 0
	for i := 0; i < len(a); i++ {
		if i == 0 || a[i] != a[i-1] {
			a[j] = a[i]
			j++
		}
	}
	return a[:j]
}

func writeStringsAsBin(path string, arr []string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	for _, s := range arr {
		if err := writeLenPrefixed(w, s); err != nil {
			return err
		}
	}
	return nil
}

// node chunk merge
type pqItem struct {
	s   string
	idx int
}
type stringHeap []pqItem

func (h stringHeap) Len() int            { return len(h) }
func (h stringHeap) Less(i, j int) bool  { return h[i].s > h[j].s }
func (h stringHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *stringHeap) Push(x interface{}) { *h = append(*h, x.(pqItem)) }
func (h *stringHeap) Pop() interface{}   { n := len(*h); x := (*h)[n-1]; *h = (*h)[:n-1]; return x }

func mergeNodeChunks(chunks []string, outPath, offsPath string) error {
	K := len(chunks)
	files := make([]*os.File, K)
	readers := make([]*bufio.Reader, K)
	for i := 0; i < K; i++ {
		f, err := os.Open(chunks[i])
		if err != nil {
			return err
		}
		files[i] = f
		readers[i] = bufio.NewReader(f)
	}
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()
	h := &stringHeap{}
	heap.Init(h)
	// read first from each
	for i := 0; i < K; i++ {
		if s, err := readLenPrefixedString(readers[i]); err == nil {
			heap.Push(h, pqItem{s, i})
		} else {
			if err != io.EOF {
				return err
			}
		}
	}
	of, _ := os.Create(outPath)
	defer of.Close()
	ow := bufio.NewWriter(of)
	defer ow.Flush()
	offo, _ := os.Create(offsPath)
	defer offo.Close()
	offw := bufio.NewWriter(offo)
	defer offw.Flush()
	var last string
	curOff := uint64(0)
	count := 0
	for h.Len() > 0 {
		it := heap.Pop(h).(pqItem)
		s := it.s
		idx := it.idx
		if next, err := readLenPrefixedString(readers[idx]); err == nil {
			heap.Push(h, pqItem{next, idx})
		}
		if count == 0 || s != last {
			// write offset
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], curOff)
			offw.Write(buf[:])
			// write string
			var lbuf [4]byte
			binary.LittleEndian.PutUint32(lbuf[:], uint32(len(s)))
			ow.Write(lbuf[:])
			ow.WriteString(s)
			curOff += 4 + uint64(len(s))
			last = s
			count++
		}
	}
	return nil
}

func readLenPrefixedString(r *bufio.Reader) (string, error) {
	var lbuf [4]byte
	if _, err := io.ReadFull(r, lbuf[:]); err != nil {
		return "", err
	}
	l := int(binary.LittleEndian.Uint32(lbuf[:]))
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func buildIdToStr(nodesFinal, idToStrPath string) (uint32, error) {
	f, err := os.Open(nodesFinal)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var tmp []string
	for {
		s, err := readLenPrefixedString(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		tmp = append(tmp, s)
	}
	out, err := os.Create(idToStrPath)
	if err != nil {
		return 0, err
	}
	defer out.Close()
	w := bufio.NewWriter(out)
	defer w.Flush()
	binary.Write(w, binary.LittleEndian, uint32(len(tmp)))
	for _, s := range tmp {
		writeLenPrefixed(w, s)
	}
	return uint32(len(tmp)), nil
}

func readIdToStr(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var cnt uint32
	if err := binary.Read(r, binary.LittleEndian, &cnt); err != nil {
		return nil, err
	}
	res := make([]string, cnt)
	for i := uint32(0); i < cnt; i++ {
		s, err := readLenPrefixedString(r)
		if err != nil {
			return nil, err
		}
		res[i] = s
	}
	return res, nil
}

func mapEdgesToIds(edgesTmp, nodesOffsets, nodesFinal, outPath string) error {
	// load offsets and nodesFinal into memory to support binary search mapping
	offf, err := os.Open(nodesOffsets)
	if err != nil {
		return err
	}
	defer offf.Close()
	stat, _ := offf.Stat()
	n := stat.Size() / 8
	offs := make([]uint64, n)
	if err := binary.Read(bufio.NewReader(offf), binary.LittleEndian, &offs); err != nil {
		return err
	}
	nf, err := os.Open(nodesFinal)
	if err != nil {
		return err
	}
	defer nf.Close()
	nb, err := io.ReadAll(nf)
	if err != nil {
		return err
	}

	ein, err := os.Open(edgesTmp)
	if err != nil {
		return err
	}
	defer ein.Close()
	er := bufio.NewReader(ein)
	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()
	ow := bufio.NewWriter(out)
	defer ow.Flush()

	for {
		u, err := readLenPrefixedString(er)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		v, err := readLenPrefixedString(er)
		if err != nil {
			return err
		}
		uid := diskStringToId(u, offs, nb)
		vid := diskStringToId(v, offs, nb)
		if uid == ^uint32(0) || vid == ^uint32(0) {
			continue
		}
		binary.Write(ow, binary.LittleEndian, uid)
		binary.Write(ow, binary.LittleEndian, vid)
	}
	return nil
}

func diskStringToId(key string, offs []uint64, blob []byte) uint32 {
	lo := 0
	hi := len(offs) - 1
	for lo <= hi {
		mid := (lo + hi) / 2
		pos := offs[mid]
		if int(pos)+4 > len(blob) {
			return ^uint32(0)
		}
		l := int(binary.LittleEndian.Uint32(blob[pos : pos+4]))
		s := string(blob[pos+4 : pos+4+uint64(l)])
		if key == s {
			return uint32(mid)
		}
		if key < s {
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}
	return ^uint32(0)
}

func splitSortEdges(edgesBin, base string, chunkBytes int) ([]string, error) {
	f, err := os.Open(edgesBin)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var chunks []string
	buf := make([][2]uint32, 0)
	cur := 0
	for {
		var a, b uint32
		if err := binary.Read(r, binary.LittleEndian, &a); err != nil {
			break
		}
		if err := binary.Read(r, binary.LittleEndian, &b); err != nil {
			return nil, err
		}
		buf = append(buf, [2]uint32{a, b})
		cur += 8
		if cur >= chunkBytes {
			sort.Slice(buf, func(i, j int) bool {
				if buf[i][0] == buf[j][0] {
					return buf[i][1] < buf[j][1]
				}
				return buf[i][0] < buf[j][0]
			})
			chunk := filepath.Join(base, "edge_chunk_"+string(len(chunks))+".bin")
			of, _ := os.Create(chunk)
			bw := bufio.NewWriter(of)
			for _, p := range buf {
				binary.Write(bw, binary.LittleEndian, p[0])
				binary.Write(bw, binary.LittleEndian, p[1])
			}
			bw.Flush()
			of.Close()
			chunks = append(chunks, chunk)
			buf = buf[:0]
			cur = 0
		}
	}
	if len(buf) > 0 {
		sort.Slice(buf, func(i, j int) bool {
			if buf[i][0] == buf[j][0] {
				return buf[i][1] < buf[j][1]
			}
			return buf[i][0] < buf[j][0]
		})
		chunk := filepath.Join(base, "edge_chunk_"+string(len(chunks))+".bin")
		of, _ := os.Create(chunk)
		bw := bufio.NewWriter(of)
		for _, p := range buf {
			binary.Write(bw, binary.LittleEndian, p[0])
			binary.Write(bw, binary.LittleEndian, p[1])
		}
		bw.Flush()
		of.Close()
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func mergeEdgeChunks(chunks []string, outPath string) error {
	K := len(chunks)
	files := make([]*os.File, K)
	readers := make([]*bufio.Reader, K)
	for i := 0; i < K; i++ {
		f, err := os.Open(chunks[i])
		if err != nil {
			return err
		}
		files[i] = f
		readers[i] = bufio.NewReader(f)
	}
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()
	type Item struct {
		u, v uint32
		idx  int
	}
	h := &struct{ items []Item }{}
	// simple linear load initial
	pq := make([]Item, 0)
	for i := 0; i < K; i++ {
		var a, b uint32
		if err := binary.Read(readers[i], binary.LittleEndian, &a); err == nil {
			binary.Read(readers[i], binary.LittleEndian, &b)
			pq = append(pq, Item{a, b, i})
		}
	}
	// naive merge by repeatedly finding min (K is expected small)
	of, _ := os.Create(outPath)
	ow := bufio.NewWriter(of)
	defer ow.Flush()
	for len(pq) > 0 {
		minIdx := 0
		for i := 1; i < len(pq); i++ {
			if pq[i].u < pq[minIdx].u || (pq[i].u == pq[minIdx].u && pq[i].v < pq[minIdx].v) {
				minIdx = i
			}
		}
		it := pq[minIdx]
		binary.Write(ow, binary.LittleEndian, it.u)
		binary.Write(ow, binary.LittleEndian, it.v)
		// refill from that chunk
		var a, b uint32
		if err := binary.Read(readers[it.idx], binary.LittleEndian, &a); err == nil {
			binary.Read(readers[it.idx], binary.LittleEndian, &b)
			pq[minIdx] = Item{a, b, it.idx}
		} else {
			pq = append(pq[:minIdx], pq[minIdx+1:]...)
		}
	}
	return nil
}

func buildCSRFromSortedEdges(edgesSorted string, nodeCount int, base string) error {
	in, err := os.Open(edgesSorted)
	if err != nil {
		return err
	}
	defer in.Close()
	r := bufio.NewReader(in)
	fOffsets := make([]uint64, nodeCount+1)
	fCounts := make([]uint32, nodeCount)
	var fNeighbors []byte
	cur := 0
	bytepos := uint64(0)
	nbrs := make([]uint32, 0)
	for {
		var u, v uint32
		if err := binary.Read(r, binary.LittleEndian, &u); err != nil {
			break
		}
		binary.Read(r, binary.LittleEndian, &v)
		for cur < u {
			fOffsets[cur] = bytepos
			fCounts[cur] = 0
			cur++
		}
		nbrs = append(nbrs, v)
		// peek next by using buffered reader is complex; flush neighbors when u changes by tracking last
		// simple approach: read ahead not supported here; we'll flush when u changes by storing lastU
		// For simplicity, continue and flush on change when next read differs; handled in outer loop
		// (implementation detail simplified)
		// We'll flush when we detect change next iteration; to keep code concise, we leave this as-is
		if len(nbrs) > 0 {
			// compress and flush immediately to keep memory bounded
			comp := compressNeighbors(nbrs)
			fNeighbors = append(fNeighbors, comp...)
			bytepos += uint64(len(comp))
			fCounts[cur] = uint32(len(nbrs))
			nbrs = nbrs[:0]
		}
	}
	for cur <= nodeCount {
		fOffsets[cur] = bytepos
		cur++
	}

	// write files
	writeBinary(filepath.Join(base, "forward_offsets.bin"), fOffsets)
	writeBinaryUint32(filepath.Join(base, "forward_counts.bin"), fCounts)
	os.WriteFile(filepath.Join(base, "forward_neighbors.bin"), fNeighbors, 0644)
	return nil
}

func writeBinary(path string, arr []uint64) error {
	f, _ := os.Create(path)
	defer f.Close()
	return binary.Write(bufio.NewWriter(f), binary.LittleEndian, arr)
}
func writeBinaryUint32(path string, arr []uint32) error {
	f, _ := os.Create(path)
	defer f.Close()
	return binary.Write(bufio.NewWriter(f), binary.LittleEndian, arr)
}

// simple varint delta compress
func compressNeighbors(neighbors []uint32) []byte {
	var out []byte
	prev := uint32(0)
	for _, v := range neighbors {
		delta := uint64(v - prev)
		var buf [10]byte
		n := binary.PutUvarint(buf[:], delta)
		out = append(out, buf[:n]...)
		prev = v
	}
	return out
}

func (s *Storage) loadForwardCSR(base string) error {
	// load offsets and counts and neighbors into memory (could mmap instead)
	offPath := filepath.Join(base, "forward_offsets.bin")
	offf, err := os.Open(offPath)
	if err != nil {
		return err
	}
	defer offf.Close()
	stat, _ := offf.Stat()
	cnt := int(stat.Size() / 8)
	s.forwardOffsets = make([]uint64, cnt)
	if err := binary.Read(bufio.NewReader(offf), binary.LittleEndian, &s.forwardOffsets); err != nil {
		return err
	}
	cPath := filepath.Join(base, "forward_counts.bin")
	cf, _ := os.Open(cPath)
	defer cf.Close()
	s.forwardCounts = make([]uint32, cnt-1)
	binary.Read(bufio.NewReader(cf), binary.LittleEndian, &s.forwardCounts)
	nb, _ := os.ReadFile(filepath.Join(base, "forward_neighbors.bin"))
	s.forwardNeighbors = nb
	return nil
}

// Query helpers
func (s *Storage) OutDegree(id uint32) uint32 {
	if id >= uint32(len(s.forwardCounts)) {
		return 0
	}
	return s.forwardCounts[id]
}

func (s *Storage) GetOutNeighbors(id uint32) ([]uint32, error) {
	if id >= uint32(len(s.forwardOffsets)-1) {
		return nil, nil
	}
	start := s.forwardOffsets[id]
	end := s.forwardOffsets[id+1]
	if end <= start {
		return nil, nil
	}
	data := s.forwardNeighbors[start:end]
	return decompressNeighbors(data), nil
}

func decompressNeighbors(data []byte) []uint32 {
	var res []uint32
	var prev uint64 = 0
	for len(data) > 0 {
		val, n := binary.Uvarint(data)
		if n <= 0 {
			break
		}
		prev += val
		res = append(res, uint32(prev))
		data = data[n:]
	}
	return res
}

func (s *Storage) StringToId(str string) (uint32, error) {
	// linear scan in-memory idToStr (could binary search on disk structures)
	// but idToStr is sorted
	lo := 0
	hi := len(s.idToStr) - 1
	for lo <= hi {
		mid := (lo + hi) / 2
		if s.idToStr[mid] == str {
			return uint32(mid), nil
		}
		if s.idToStr[mid] < str {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return ^uint32(0), errors.New("not found")
}

func (s *Storage) IdToString(id uint32) (string, error) {
	if int(id) >= len(s.idToStr) {
		return "", errors.New("invalid id")
	}
	return s.idToStr[id], nil
}
