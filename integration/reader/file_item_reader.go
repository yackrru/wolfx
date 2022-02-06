package reader

import (
	"context"
	"encoding/csv"
	"github.com/ttksm/wolfx/middleware"
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
	Reader *csv.Reader

	// If HasHeader is true, FileItemReader will ignore the 1st line of file.
	// If false, FileItemReader will read from 1st line of file.
	HasHeader bool

	// ChunkSize is the number of rows to be read at once.
	// If specify 0, FileItemReader will read all lines of file at once.
	ChunkSize uint
}

func NewFileItemReader(config *FileItemReaderConfig) *FileItemReader {
	return &FileItemReader{
		config: config,
	}
}

func (r *FileItemReader) Read(ctx context.Context, ch chan<- interface{}) error {
	reader := r.config.Reader

	var header []string
	if r.config.HasHeader {
		var err error
		if header, err = reader.Read(); err != nil {
			return err
		}
	}

	if r.config.ChunkSize > 0 {
		/*
			for {
				record, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
			}
		*/
	} else {
		records, err := reader.ReadAll()
		if err != nil {
			return err
		}
		var chunk []map[string]string
		for _, record := range records {
			resultSet := make(map[string]string)
			for idx, val := range record {
				var key string
				if header == nil {
					key = strconv.Itoa(idx)
				} else {
					key = header[idx]
				}
				resultSet[key] = val
			}
			chunk = append(chunk, resultSet)
		}
		ch <- chunk
	}

	return nil
}
