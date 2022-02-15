package file

import (
	"context"
	"fmt"
	"github.com/yackrru/wolfx/middleware"
	"reflect"
	"sort"
)

var _ middleware.Writer = new(Writer)

// Writer is an implementation of middleware.Writer.
// It contains the standard package encoding/csv
// and is used to write csv format files.
type Writer struct {
	conf *WriterConfig
}

// WriterConfig is the configuration of Writer.
type WriterConfig struct {
	Writer CSVWriter

	// PropsBindPosition is the position mapping of header's columns.
	// Key of map is property (column) name and value is position with starting 0.
	PropsBindPosition middleware.PropsBindPosition

	// If NoHeader is true, Writer firstly outputs header string to csv.
	NoHeader bool
}

// CSVWriter is the interface that wraps methods Write and WriteAll.
// Write is used to output the header string
// and WriteAll is used to output the body string.
type CSVWriter interface {
	Write(record []string) error
	WriteAll(records [][]string) error
	Flush()
	Error() error
}

func NewWriter(conf *WriterConfig) *Writer {
	return &Writer{
		conf: conf,
	}
}

func (w *Writer) Write(ctx context.Context, ch <-chan interface{}) error {
	writer := w.conf.Writer

	if !w.conf.NoHeader {
		header := generateHeader(w.conf.PropsBindPosition)
		if err := writer.Write(header); err != nil {
			return err
		}
	}

	for chunk := range ch {
		var items [][]string
		switch chunk.(type) {
		case []middleware.MapMapperType:
			items = middleware.MapMapperToFlatItems(chunk.([]middleware.MapMapperType),
				w.conf.PropsBindPosition)
		case []middleware.CustomMapperType:
			items = middleware.CustomMapperToFlatItems(chunk.([]middleware.CustomMapperType),
				w.conf.PropsBindPosition)
		default:
			v := reflect.ValueOf(chunk)
			return fmt.Errorf("Not supported such a chunk type: %s", v.Type())
		}
		if err := writer.WriteAll(items); err != nil {
			return err
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			return err
		}
	}

	return nil
}

func generateHeader(propsBindPosition map[string]uint) []string {
	reverted := make(map[int]string, len(propsBindPosition))

	var idxList []int
	for k, v := range propsBindPosition {
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
