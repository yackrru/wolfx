package file

import (
	"context"
	"github.com/yackrru/wolfx/middleware"
	"io"
	"strconv"
)

var _ middleware.Reader = new(Reader)

// Reader is an implementation of middleware.Reader.
// It contains the standard package encoding/csv
// and is used to read csv format files.
type Reader struct {
	conf *ReaderConfig
}

// ReaderConfig is the configuration of Reader.
type ReaderConfig struct {
	Reader CSVReader

	// If HasHeader is true, Reader will ignore the 1st line of file.
	// If false, Reader will read from 1st line of file.
	HasHeader bool

	// ChunkSize is the number of rows to be read at once.
	// If specify 0, Reader will read all lines of file at once.
	ChunkSize uint

	// RowMapperFunc is the mapping function.
	// If it is nil, Reader will send data as the type
	// of MapMapperType to channel.
	// If not nil, Reader will send data as the type that user defined.
	RowMapperFunc RowMapper
}

// CSVReader is the interface that wraps methods of Read and ReadAll.
// Read reads from a file line by line and ReadAll is the opposite.
type CSVReader interface {
	Read() (record []string, err error)
	ReadAll() (records [][]string, err error)
}

// RowMapper is the function type to map csv rows to user's own struct.
type RowMapper func(ctx context.Context, ch chan<- interface{},
	chunk []middleware.MapMapperType) error

func NewReader(conf *ReaderConfig) *Reader {
	return &Reader{
		conf: conf,
	}
}

func (r *Reader) Read(ctx context.Context, ch chan<- interface{}) error {
	defer close(ch)

	reader := r.conf.Reader
	var header []string
	if r.conf.HasHeader {
		var err error
		if header, err = reader.Read(); err != nil {
			return err
		}
	}

	if r.conf.ChunkSize > 0 {
		for {
			var chunk []middleware.MapMapperType
			for i := 0; i < int(r.conf.ChunkSize); i++ {
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
		var chunk []middleware.MapMapperType
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

func (r *Reader) sendChunk(ctx context.Context, ch chan<- interface{},
	chunk []middleware.MapMapperType) error {

	if r.conf.RowMapperFunc == nil {
		ch <- chunk
	} else {
		if err := r.conf.RowMapperFunc(ctx, ch, chunk); err != nil {
			return err
		}
	}

	return nil
}

func createResultSet(header []string, record []string) middleware.MapMapperType {
	resultSet := make(middleware.MapMapperType)

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
