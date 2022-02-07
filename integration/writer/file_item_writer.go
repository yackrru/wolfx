package writer

import (
	"context"
	"encoding/csv"
	"github.com/ttksm/wolfx/integration/reader"
	"github.com/ttksm/wolfx/middleware"
	"sort"
)

var _ middleware.Writer = new(FileItemWriter)

// FileItemWriter is an implementation of middleware.Writer.
// It contains the standard package encoding/csv
// and is used to write csv format files.
type FileItemWriter struct {
	config *FileItemWriterConfig
}

// FileItemWriterConfig is the configuration of FileItemWriter.
type FileItemWriterConfig struct {
	Writer CSVWriter

	// PropertiesBindPosition is the position mapping of header's columns.
	// Key of map is property (column) name and value is position with starting 0.
	PropertiesBindPosition map[string]uint

	// If NoHeader is true, FileItemWriter firstly outputs header string to csv.
	NoHeader bool
}

// CSVWriter is the interface that wraps methods Write and WriteAll.
// Write is used to output the header string
// and WriteAll is used to output the body string.
type CSVWriter interface {
	Write(record []string) error
	WriteAll(records [][]string) error
}

func NewFileItemWriter(config *FileItemWriterConfig) *FileItemWriter {
	return &FileItemWriter{
		config: config,
	}
}

func NewFileItemWriterConfig(writer *csv.Writer,
	propertiesBindPosition map[string]uint) *FileItemWriterConfig {
	return &FileItemWriterConfig{
		Writer:                 writer,
		PropertiesBindPosition: propertiesBindPosition,
	}
}

func (w *FileItemWriter) Write(ctx context.Context, ch <-chan interface{}) error {
	writer := w.config.Writer

	if !w.config.NoHeader {
		header := generateHeader(w.config.PropertiesBindPosition)
		if err := writer.Write(header); err != nil {
			return err
		}
	}

	for chunk := range ch {
		var items [][]string
		switch chunk.(type) {
		case []reader.MapMapperType:
			items = convertItems(chunk.([]reader.MapMapperType),
				w.config.PropertiesBindPosition)
		default:
			// TODO
		}
		if err := writer.WriteAll(items); err != nil {
			return err
		}
	}

	return nil
}

func generateHeader(propertiesBindPosition map[string]uint) []string {
	reverted := make(map[int]string, len(propertiesBindPosition))

	var idxList []int
	for k, v := range propertiesBindPosition {
		idxList = append(idxList, int(v))
		reverted[int(v)] = k
	}
	sort.Ints(idxList)

	var header []string
	for _, idx := range idxList {
		header = append(header, reverted[idx])
	}

	return header
}

func convertItems(chunk []reader.MapMapperType,
	propertiesBindPosition map[string]uint) [][]string {

	var items [][]string
	for _, itemBuf := range chunk {
		itemMap := make(map[int]string)
		for k, v := range itemBuf {
			position := int(propertiesBindPosition[k])
			itemMap[position] = v
		}

		var idxList []int
		for k := range itemMap {
			idxList = append(idxList, k)
		}
		sort.Ints(idxList)

		var item []string
		for _, idx := range idxList {
			item = append(item, itemMap[idx])
		}
		items = append(items, item)
	}

	return items
}
