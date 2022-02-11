package file

import (
	"context"
	"encoding/csv"
	"github.com/stretchr/testify/assert"
	"github.com/yackrru/wolfx/middleware"
	"io"
	"os"
	"sync"
	"testing"
)

func TestReaderForHeader(t *testing.T) {
	t.Run("csv with header", func(t *testing.T) {
		file, err := os.Open("testdata/test_with_header.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		csvReader := csv.NewReader(file)

		reader := NewReader(&ReaderConfig{
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
		chunk := iChunk.([]middleware.MapMapperType)

		assertionFileReader(t, chunk, "id", "name", "created_at")
	})

	t.Run("csv without header", func(t *testing.T) {
		file, err := os.Open("testdata/test_without_header.csv")
		if err != nil {
			t.Fatal(err)
		}
		csvReader := csv.NewReader(file)

		reader := NewReader(&ReaderConfig{
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
		chunk := iChunk.([]middleware.MapMapperType)

		assertionFileReader(t, chunk, "0", "1", "2")
	})
}

func assertionFileReader(t *testing.T, chunk []middleware.MapMapperType, keys ...string) {
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

func TestReaderForChunkSize(t *testing.T) {
	file, err := os.Open("testdata/test_with_header.csv")
	if err != nil {
		t.Fatal(err)
	}
	csvReader := csv.NewReader(file)

	reader := NewReader(&ReaderConfig{
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
	var wantList [][]middleware.MapMapperType
	for {
		var want []middleware.MapMapperType
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
		chunk := iChunk.([]middleware.MapMapperType)
		assert.ElementsMatch(t, wantList[assertTimes], chunk)
		assertTimes++
	}
}

func TestReaderWithMapperFunc(t *testing.T) {
	t.Run("Without ChunkSize", func(t *testing.T) {
		file, err := os.Open("testdata/test_with_header.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		csvReader := csv.NewReader(file)

		reader := NewReader(&ReaderConfig{
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
		chunk := iChunk.([]middleware.CustomMapperType)

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
			got := chunk[count].Props.(TestCSVMapping)
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

		reader := NewReader(&ReaderConfig{
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

		var wantList [][]middleware.CustomMapperType
		for {
			var want []middleware.CustomMapperType
			for i := 0; i < 5; i++ {
				wantRow, err := csvWantReader.Read()
				if err == io.EOF {
					wantList = append(wantList, want)
					goto Exit
				}
				if err != nil {
					t.Fatal(err)
				}
				resultSet := middleware.CustomMapperType{
					Props: TestCSVMapping{
						Id:        wantRow[0],
						Name:      wantRow[1],
						CreatedAt: wantRow[2],
					},
				}
				want = append(want, resultSet)
			}
			wantList = append(wantList, want)
		}
	Exit:

		var assertTimes int
		for iChunk := range ch {
			chunk := iChunk.([]middleware.CustomMapperType)
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

func CSVMapper(ctx context.Context, ch chan<- interface{},
	chunk []middleware.MapMapperType) error {
	var list []middleware.CustomMapperType

	for _, row := range chunk {
		dto := middleware.CustomMapperType{
			Props: TestCSVMapping{
				Id:        row["id"],
				Name:      row["name"],
				CreatedAt: row["created_at"],
			},
		}
		list = append(list, dto)
	}

	ch <- list

	return nil
}
