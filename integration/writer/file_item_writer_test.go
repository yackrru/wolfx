package writer

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/yackrru/wolfx/integration/reader"
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

type TestChunkType struct {
	Id   string `csvprop:"id"`
	Name string `csvprop:"name"`
}

func TestWrite(t *testing.T) {
	propertiesBindPosition := make(map[string]uint)
	propertiesBindPosition["id"] = 0
	propertiesBindPosition["name"] = 1

	t.Run("MapMapperType with header", func(t *testing.T) {
		csvWriter := &TestCSVWriter{}
		config := &FileItemWriterConfig{
			Writer:                 csvWriter,
			PropertiesBindPosition: propertiesBindPosition,
		}
		writer := NewFileItemWriter(config)

		var wg sync.WaitGroup
		ch := make(chan interface{})
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			if err := writer.Write(context.TODO(), ch); err != nil {
				t.Fatal(err)
			}
		}(ch)

		chunk := []reader.MapMapperType{
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
		config := &FileItemWriterConfig{
			Writer:                 csvWriter,
			PropertiesBindPosition: propertiesBindPosition,
			NoHeader:               true,
		}
		writer := NewFileItemWriter(config)

		var wg sync.WaitGroup
		ch := make(chan interface{})
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			if err := writer.Write(context.TODO(), ch); err != nil {
				t.Error(err)
			}
		}(ch)

		chunk := []reader.CustomMapperType{
			{
				Properties: TestChunkType{
					Id:   "0",
					Name: "name0",
				},
			},
			{
				Properties: TestChunkType{
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
		config := &FileItemWriterConfig{
			Writer:                 csvWriter,
			PropertiesBindPosition: propertiesBindPosition,
			NoHeader:               true,
		}
		writer := NewFileItemWriter(config)

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
		config := &FileItemWriterConfig{
			Writer:                 csvWriter,
			PropertiesBindPosition: propertiesBindPosition,
			NoHeader:               true,
		}
		writer := NewFileItemWriter(config)

		var wg sync.WaitGroup
		ch := make(chan interface{})
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			if err := writer.Write(context.TODO(), ch); err != nil {
				t.Fatal(err)
			}
		}(ch)

		chunk := []reader.MapMapperType{
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
	propertiesBindPosition := make(map[string]uint)
	propertiesBindPosition["name"] = 1
	propertiesBindPosition["created_at"] = 2
	propertiesBindPosition["id"] = 0

	header := generateHeader(propertiesBindPosition)
	for idx, property := range header {
		assert.Equal(t, int(propertiesBindPosition[property]), idx)
	}
}

func TestConvertItemsMapMapper(t *testing.T) {
	chunk := []reader.MapMapperType{
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
	propertiesBindPosition := make(map[string]uint)
	propertiesBindPosition["created_at"] = 0
	propertiesBindPosition["id"] = 1
	propertiesBindPosition["name"] = 2

	items := convertItemsMapMapper(chunk, propertiesBindPosition)
	for idx, item := range items {
		target := chunk[idx]
		assert.Equal(t, target["created_at"], item[0])
		assert.Equal(t, target["id"], item[1])
		assert.Equal(t, target["name"], item[2])
	}
}

func TestConvertItemsCustomMapper(t *testing.T) {
	chunk := []reader.CustomMapperType{
		{
			Properties: TestChunkType{
				Id:   "0",
				Name: "name0",
			},
		},
		{
			Properties: TestChunkType{
				Id:   "1",
				Name: "name1",
			},
		},
		{
			Properties: TestChunkType{
				Id:   "2",
				Name: "name2",
			},
		},
	}
	propertiesBindPosition := make(map[string]uint)
	propertiesBindPosition["id"] = 2
	propertiesBindPosition["name"] = 1

	items := convertItemsCustomMapper(chunk, propertiesBindPosition)
	for idx, item := range items {
		target := chunk[idx]
		assert.Equal(t, target.Properties.(TestChunkType).Id, item[1])
		assert.Equal(t, target.Properties.(TestChunkType).Name, item[0])
	}
}
