package storage

type Storage struct {
	BaseDir   string
	NodeCount uint32
	EdgeCount uint64

	forwardOffsets   []uint64
	forwardNeighbors []byte
	forwardCounts    []uint32
}

func BuildFromCSV(baseDir, csvPath string) (*Storage, error) {

}
