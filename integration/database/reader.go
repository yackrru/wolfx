package database

import (
	"context"
	"database/sql"
	"github.com/yackrru/wolfx/middleware"
)

var _ middleware.Reader = new(Reader)

// Reader is an implementation of middleware.Reader.
// It is used to read data by using cursor from database.
type Reader struct {
	conf *ReaderConfig
}

// ReaderConfig is the configuration of Reader.
type ReaderConfig struct {
	DB *sql.DB

	// SQL is a select dml string.
	SQL string

	// ChunkSize is the number of rows to be sent to writer at once.
	// If specify 0, Reader will send all fetched rows at once.
	ChunkSize uint

	// RowMapperFunc is the mapping function.
	// If it is nil, Reader will send data as the type
	// of MapMapperType to channel.
	// If not nil, Reader will send data as the type that user defined.
	RowMapperFunc middleware.RowMapper
}

func NewReader(conf *ReaderConfig) *Reader {
	return &Reader{
		conf: conf,
	}
}

func (r *Reader) Read(ctx context.Context, ch chan<- interface{}) error {
	defer close(ch)

	rows, err := r.conf.DB.Query(r.conf.SQL)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	vals := make([]interface{}, len(cols))
	for i, _ := range cols {
		vals[i] = new(sql.RawBytes)
	}

	if r.conf.ChunkSize > 0 {
		cursor := 0
		var chunk []middleware.MapMapperType
		for rows.Next() {
			cursor++
			if err := rows.Scan(vals...); err != nil {
				return err
			}
			resultSet := rawBytesToMapMapper(cols, vals)
			chunk = append(chunk, resultSet)
			if cursor%int(r.conf.ChunkSize) == 0 {
				ch <- chunk
				chunk = []middleware.MapMapperType{}
			}
		}
		if len(chunk) != 0 {
			ch <- chunk
		}
	} else {
		var chunk []middleware.MapMapperType
		for rows.Next() {
			if err := rows.Scan(vals...); err != nil {
				return err
			}
			resultSet := rawBytesToMapMapper(cols, vals)
			chunk = append(chunk, resultSet)
		}
		ch <- chunk
	}

	return nil
}

func rawBytesToMapMapper(cols []string, vals []interface{},
) middleware.MapMapperType {
	resultSet := make(middleware.MapMapperType)

	for i, col := range cols {
		val := vals[i].(*sql.RawBytes)
		resultSet[col] = string(*val)
	}

	return resultSet
}
