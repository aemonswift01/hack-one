package storage_test

import (
	"testing"

	"github.com/aemonswift01/hack-one/storage"
)

func TestBuild(t *testing.T) {
	s := storage.Storage{
		BaseDir: "../test",
	}
	if err := s.BuildFromCSV("../data/data_10.csw"); err != nil {
		t.Fatalf("BuildFromCSV failed: %v", err)
	}
}
