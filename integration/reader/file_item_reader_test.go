package reader

import (
	"context"
	"encoding/csv"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"sync"
	"testing"
)

func TestFileItemReaderForHeader(t *testing.T) {
	t.Run("csv with header", func(t *testing.T) {
		file, err := os.Open("testdata/test_with_header.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		csvReader := csv.NewReader(file)

		reader := NewFileItemReader(&FileItemReaderConfig{
			Reader:    csvReader,
			HasHeader: true,
		})

		var wg sync.WaitGroup
		ch := make(chan interface{})
		defer close(ch)
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			if err := reader.Read(context.TODO(), ch); err != nil {
				t.Fatal(err)
			}
		}(ch)
		iChunk := <-ch
		chunk := iChunk.([]map[string]string)

		assertionFileReader(t, chunk, "id", "name", "created_at")
	})

	t.Run("csv without header", func(t *testing.T) {
		file, err := os.Open("testdata/test_without_header.csv")
		if err != nil {
			t.Fatal(err)
		}
		csvReader := csv.NewReader(file)

		reader := NewFileItemReader(&FileItemReaderConfig{
			Reader: csvReader,
		})

		var wg sync.WaitGroup
		ch := make(chan interface{})
		defer close(ch)
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			if err := reader.Read(context.TODO(), ch); err != nil {
				t.Fatal(err)
			}
		}(ch)
		iChunk := <-ch
		chunk := iChunk.([]map[string]string)

		assertionFileReader(t, chunk, "0", "1", "2")
	})
}

func assertionFileReader(t *testing.T, chunk []map[string]string, keys ...string) {
	fileWant, err := os.Open("testdata/test_without_header.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer fileWant.Close()
	csvWantReader := csv.NewReader(fileWant)

	count := 0
	for {
		wanted, err := csvWantReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got := chunk[count]
		assert.Equal(t, wanted[0], got[keys[0]])
		assert.Equal(t, wanted[1], got[keys[1]])
		assert.Equal(t, wanted[2], got[keys[2]])
		count++
	}
}
