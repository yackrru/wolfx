package file

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/yackrru/wolfx/middleware"
	"sync"
	"testing"
)

type TestCSVWriter struct {
	header []string
	result [][]string
}

func (w *TestCSVWriter) Write(record []string) error {
	w.header = record
	return nil
}

func (w *TestCSVWriter) WriteAll(records [][]string) error {
	for _, record := range records {
		w.result = append(w.result, record)
	}
	return nil
}

func (w *TestCSVWriter) Flush() {}

func (w *TestCSVWriter) Error() error {
	return nil
}

type TestChunkType struct {
	Id   string `prop:"id"`
	Name string `prop:"name"`
}

func TestWrite(t *testing.T) {
	propsBindPosition := make(map[string]uint)
	propsBindPosition["id"] = 0
	propsBindPosition["name"] = 1

	t.Run("MapMapperType with header", func(t *testing.T) {
		csvWriter := &TestCSVWriter{}
		config := &WriterConfig{
			Writer:            csvWriter,
			PropsBindPosition: propsBindPosition,
		}
		writer := NewWriter(config)

		var wg sync.WaitGroup
		ch := make(chan interface{})
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			if err := writer.Write(context.TODO(), ch); err != nil {
				t.Fatal(err)
			}
		}(ch)

		chunk := []middleware.MapMapperType{
			{"id": "0", "name": "name0"},
			{"id": "1", "name": "name1"},
		}
		ch <- chunk
		ch <- chunk
		ch <- chunk
		close(ch)
		wg.Wait()

		assert.ElementsMatch(t, []string{"id", "name"}, csvWriter.header)
		assert.ElementsMatch(t, [][]string{
			{"0", "name0"},
			{"1", "name1"},
			{"0", "name0"},
			{"1", "name1"},
			{"0", "name0"},
			{"1", "name1"},
		}, csvWriter.result)
	})

	t.Run("MapMapperType without header", func(t *testing.T) {
		csvWriter := &TestCSVWriter{}
		config := &WriterConfig{
			Writer:            csvWriter,
			PropsBindPosition: propsBindPosition,
			NoHeader:          true,
		}
		writer := NewWriter(config)

		var wg sync.WaitGroup
		ch := make(chan interface{})
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			if err := writer.Write(context.TODO(), ch); err != nil {
				t.Error(err)
			}
		}(ch)

		chunk := []middleware.CustomMapperType{
			{
				Props: TestChunkType{
					Id:   "0",
					Name: "name0",
				},
			},
			{
				Props: TestChunkType{
					Id:   "1",
					Name: "name1",
				},
			},
		}
		ch <- chunk
		ch <- chunk
		ch <- chunk
		close(ch)
		wg.Wait()

		assert.Empty(t, csvWriter.header)
		assert.ElementsMatch(t, [][]string{
			{"0", "name0"},
			{"1", "name1"},
			{"0", "name0"},
			{"1", "name1"},
			{"0", "name0"},
			{"1", "name1"},
		}, csvWriter.result)
	})

	t.Run("Not found chunk type", func(t *testing.T) {
		csvWriter := &TestCSVWriter{}
		config := &WriterConfig{
			Writer:            csvWriter,
			PropsBindPosition: propsBindPosition,
			NoHeader:          true,
		}
		writer := NewWriter(config)

		var wg sync.WaitGroup
		ch := make(chan interface{})
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			err := writer.Write(context.TODO(), ch)
			if err == nil {
				t.Fatal("Want error but got nil")
			}
			assert.EqualError(t, err,
				"Not supported such a chunk type: []map[string]string")
		}(ch)

		chunk := []map[string]string{
			{"id": "0", "name": "name0"},
			{"id": "1", "name": "name1"},
		}
		ch <- chunk
		close(ch)
		wg.Wait()
	})

	t.Run("Extractor type without header", func(t *testing.T) {
		csvWriter := &TestCSVWriter{}
		config := &WriterConfig{
			Writer:            csvWriter,
			PropsBindPosition: propsBindPosition,
			NoHeader:          true,
		}
		writer := NewWriter(config)

		var wg sync.WaitGroup
		ch := make(chan interface{})
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			if err := writer.Write(context.TODO(), ch); err != nil {
				t.Fatal(err)
			}
		}(ch)

		chunk := []middleware.MapMapperType{
			{"id": "0", "name": "name0"},
			{"id": "1", "name": "name1"},
		}
		ch <- chunk
		ch <- chunk
		ch <- chunk
		close(ch)
		wg.Wait()

		assert.Empty(t, csvWriter.header)
		assert.ElementsMatch(t, [][]string{
			{"0", "name0"},
			{"1", "name1"},
			{"0", "name0"},
			{"1", "name1"},
			{"0", "name0"},
			{"1", "name1"},
		}, csvWriter.result)
	})
}

func TestGenerateHeader(t *testing.T) {
	propsBindPosition := make(map[string]uint)
	propsBindPosition["name"] = 1
	propsBindPosition["created_at"] = 2
	propsBindPosition["id"] = 0

	header := generateHeader(propsBindPosition)
	for idx, property := range header {
		assert.Equal(t, int(propsBindPosition[property]), idx)
	}
}

func TestConvertItemsMapMapper(t *testing.T) {
	chunk := []middleware.MapMapperType{
		{
			"id":         "0",
			"name":       "name0",
			"created_at": "2022-01-01 00:00:00",
		},
		{
			"id":         "1",
			"name":       "name1",
			"created_at": "2022-01-01 00:00:00",
		},
		{
			"id":         "2",
			"name":       "name2",
			"created_at": "2022-01-01 00:00:00",
		},
	}
	propsBindPosition := make(map[string]uint)
	propsBindPosition["created_at"] = 0
	propsBindPosition["id"] = 1
	propsBindPosition["name"] = 2

	items := convertItemsMapMapper(chunk, propsBindPosition)
	for idx, item := range items {
		target := chunk[idx]
		assert.Equal(t, target["created_at"], item[0])
		assert.Equal(t, target["id"], item[1])
		assert.Equal(t, target["name"], item[2])
	}
}

func TestConvertItemsCustomMapper(t *testing.T) {
	chunk := []middleware.CustomMapperType{
		{
			Props: TestChunkType{
				Id:   "0",
				Name: "name0",
			},
		},
		{
			Props: TestChunkType{
				Id:   "1",
				Name: "name1",
			},
		},
		{
			Props: TestChunkType{
				Id:   "2",
				Name: "name2",
			},
		},
	}
	propsBindPosition := make(map[string]uint)
	propsBindPosition["id"] = 2
	propsBindPosition["name"] = 1

	items := convertItemsCustomMapper(chunk, propsBindPosition)
	for idx, item := range items {
		target := chunk[idx]
		assert.Equal(t, target.Props.(TestChunkType).Id, item[1])
		assert.Equal(t, target.Props.(TestChunkType).Name, item[0])
	}
}