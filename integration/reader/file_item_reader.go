package reader

import (
	"context"
	"github.com/yackrru/wolfx/middleware"
	"io"
	"strconv"
)

var _ middleware.Reader = new(FileItemReader)

// FileItemReader is an implementation of middleware.Reader.
// It contains the standard package encoding/csv
// and is used to read csv format files.
type FileItemReader struct {
	config *FileItemReaderConfig
}

// FileItemReaderConfig is the configuration of FileItemReader.
type FileItemReaderConfig struct {
	Reader CSVReader

	// If HasHeader is true, FileItemReader will ignore the 1st line of file.
	// If false, FileItemReader will read from 1st line of file.
	HasHeader bool

	// ChunkSize is the number of rows to be read at once.
	// If specify 0, FileItemReader will read all lines of file at once.
	ChunkSize uint

	// RowMapperFunc is the mapping function.
	// If it is nil, FileItemReader will send data as the type
	// of MapMapperType to channel.
	// If not nil, FileItemReader will send data as the type that user defined.
	RowMapperFunc RowMapper
}

// CSVReader is the interface that wraps methods of Read and ReadAll.
// Read reads from a file line by line and ReadAll is the opposite.
type CSVReader interface {
	Read() (record []string, err error)
	ReadAll() (records [][]string, err error)
}

// MapMapperType is the default sending data type.
type MapMapperType map[string]string

// CustomMapperType is the custom sending data type.
type CustomMapperType struct {

	// Properties expects struct type.
	// Each element of the Properties struct
	// is given a csvprop tag.
	// For the csvprop value, enter the corresponding header name.
	// Example: `csvprop:"id"`
	Properties interface{}
}

// RowMapper is the function type to map csv rows to user's own struct.
type RowMapper func(ctx context.Context, ch chan<- interface{}, chunk []MapMapperType) error

func NewFileItemReader(config *FileItemReaderConfig) *FileItemReader {
	return &FileItemReader{
		config: config,
	}
}

func (r *FileItemReader) Read(ctx context.Context, ch chan<- interface{}) error {
	defer close(ch)

	reader := r.config.Reader
	var header []string
	if r.config.HasHeader {
		var err error
		if header, err = reader.Read(); err != nil {
			return err
		}
	}

	if r.config.ChunkSize > 0 {
		for {
			var chunk []MapMapperType
			for i := 0; i < int(r.config.ChunkSize); i++ {
				record, err := reader.Read()
				if err == io.EOF {
					if err := r.sendChunk(ctx, ch, chunk); err != nil {
						return err
					}
					goto Exit
				}
				if err != nil {
					return err
				}
				resultSet := createResultSet(header, record)
				chunk = append(chunk, resultSet)
			}

			if err := r.sendChunk(ctx, ch, chunk); err != nil {
				return err
			}
		}
	Exit:
	} else {
		records, err := reader.ReadAll()
		if err != nil {
			return err
		}
		var chunk []MapMapperType
		for _, record := range records {
			resultSet := createResultSet(header, record)
			chunk = append(chunk, resultSet)
		}
		if err := r.sendChunk(ctx, ch, chunk); err != nil {
			return err
		}
	}

	return nil
}

func (r *FileItemReader) sendChunk(ctx context.Context, ch chan<- interface{},
	chunk []MapMapperType) error {

	if r.config.RowMapperFunc == nil {
		ch <- chunk
	} else {
		if err := r.config.RowMapperFunc(ctx, ch, chunk); err != nil {
			return err
		}
	}

	return nil
}

func createResultSet(header []string, record []string) MapMapperType {
	resultSet := make(MapMapperType)

	for idx, val := range record {
		var key string
		if header == nil {
			key = strconv.Itoa(idx)
		} else {
			key = header[idx]
		}
		resultSet[key] = val
	}

	return resultSet
}
