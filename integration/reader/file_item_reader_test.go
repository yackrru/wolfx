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
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			if err := reader.Read(context.TODO(), ch); err != nil {
				t.Fatal(err)
			}
		}(ch)
		iChunk := <-ch
		chunk := iChunk.([]MapMapperType)

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
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			if err := reader.Read(context.TODO(), ch); err != nil {
				t.Fatal(err)
			}
		}(ch)
		iChunk := <-ch
		chunk := iChunk.([]MapMapperType)

		assertionFileReader(t, chunk, "0", "1", "2")
	})
}

func assertionFileReader(t *testing.T, chunk []MapMapperType, keys ...string) {
	fileWant, err := os.Open("testdata/test_without_header.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer fileWant.Close()
	csvWantReader := csv.NewReader(fileWant)

	count := 0
	for {
		want, err := csvWantReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got := chunk[count]
		assert.Equal(t, want[0], got[keys[0]])
		assert.Equal(t, want[1], got[keys[1]])
		assert.Equal(t, want[2], got[keys[2]])
		count++
	}
}

func TestFileItemReaderForChunkSize(t *testing.T) {
	file, err := os.Open("testdata/test_with_header.csv")
	if err != nil {
		t.Fatal(err)
	}
	csvReader := csv.NewReader(file)

	reader := NewFileItemReader(&FileItemReaderConfig{
		Reader:    csvReader,
		HasHeader: true,
		ChunkSize: 5,
	})

	var wg sync.WaitGroup
	ch := make(chan interface{})
	wg.Add(1)
	go func(ch chan interface{}) {
		defer wg.Done()
		if err := reader.Read(context.TODO(), ch); err != nil {
			t.Fatal(err)
		}
	}(ch)

	fileWant, err := os.Open("testdata/test_without_header.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer fileWant.Close()
	csvWantReader := csv.NewReader(fileWant)

	header := []string{"id", "name", "created_at"}
	var wantList [][]MapMapperType
	for {
		var want []MapMapperType
		for i := 0; i < 5; i++ {
			wantRow, err := csvWantReader.Read()
			if err == io.EOF {
				wantList = append(wantList, want)
				goto Exit
			}
			if err != nil {
				t.Fatal(err)
			}
			resultSet := createResultSet(header, wantRow)
			want = append(want, resultSet)
		}
		wantList = append(wantList, want)
	}
Exit:

	var assertTimes int
	for iChunk := range ch {
		chunk := iChunk.([]MapMapperType)
		assert.ElementsMatch(t, wantList[assertTimes], chunk)
		assertTimes++
	}
}

func TestFileItemReaderWithMapperFunc(t *testing.T) {
	t.Run("Without ChunkSize", func(t *testing.T) {
		file, err := os.Open("testdata/test_with_header.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		csvReader := csv.NewReader(file)

		reader := NewFileItemReader(&FileItemReaderConfig{
			Reader:        csvReader,
			HasHeader:     true,
			RowMapperFunc: CSVMapper,
		})

		var wg sync.WaitGroup
		ch := make(chan interface{})
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			if err := reader.Read(context.TODO(), ch); err != nil {
				t.Fatal(err)
			}
		}(ch)
		iChunk := <-ch
		chunk := iChunk.([]TestCSVMapping)

		fileWant, err := os.Open("testdata/test_without_header.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer fileWant.Close()
		csvWantReader := csv.NewReader(fileWant)

		count := 0
		for {
			want, err := csvWantReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			got := chunk[count]
			assert.Equal(t, want[0], got.Id)
			assert.Equal(t, want[1], got.Name)
			assert.Equal(t, want[2], got.CreatedAt)
			count++
		}
	})

	t.Run("With ChunkSize", func(t *testing.T) {
		file, err := os.Open("testdata/test_with_header.csv")
		if err != nil {
			t.Fatal(err)
		}
		csvReader := csv.NewReader(file)

		reader := NewFileItemReader(&FileItemReaderConfig{
			Reader:        csvReader,
			HasHeader:     true,
			ChunkSize:     5,
			RowMapperFunc: CSVMapper,
		})

		var wg sync.WaitGroup
		ch := make(chan interface{})
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			if err := reader.Read(context.TODO(), ch); err != nil {
				t.Fatal(err)
			}
		}(ch)

		fileWant, err := os.Open("testdata/test_without_header.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer fileWant.Close()
		csvWantReader := csv.NewReader(fileWant)

		var wantList [][]TestCSVMapping
		for {
			var want []TestCSVMapping
			for i := 0; i < 5; i++ {
				wantRow, err := csvWantReader.Read()
				if err == io.EOF {
					wantList = append(wantList, want)
					goto Exit
				}
				if err != nil {
					t.Fatal(err)
				}
				resultSet := TestCSVMapping{
					Id:        wantRow[0],
					Name:      wantRow[1],
					CreatedAt: wantRow[2],
				}
				want = append(want, resultSet)
			}
			wantList = append(wantList, want)
		}
	Exit:

		var assertTimes int
		for iChunk := range ch {
			chunk := iChunk.([]TestCSVMapping)
			assert.ElementsMatch(t, wantList[assertTimes], chunk)
			assertTimes++
		}
	})
}

type TestCSVMapping struct {
	Id        string
	Name      string
	CreatedAt string
}

func CSVMapper(ctx context.Context, ch chan<- interface{}, chunk []MapMapperType) error {
	var dto []TestCSVMapping

	for _, row := range chunk {
		obj := TestCSVMapping{
			Id:        row["id"],
			Name:      row["name"],
			CreatedAt: row["created_at"],
		}
		dto = append(dto, obj)
	}

	ch <- dto

	return nil
}
