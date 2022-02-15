package database

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/yackrru/wolfx/middleware"
	"reflect"
)

// Writer is an implementation of middleware.Writer.
// It is used to write data to database.
type Writer struct {
	conf *WriterConfig
}

// WriterConfig is the configuration of Writer.
type WriterConfig struct {
	DB *sql.DB

	// SQL is a insert dml string.
	SQL string

	// If Transactional is true, Writer writes data under transaction.
	Transactional bool

	// PropsBindPosition is the position mapping of columns.
	// Keys are column names and values are positions starting from 0.
	PropsBindPosition map[string]uint
}

func NewWriter(conf *WriterConfig) *Writer {
	return &Writer{
		conf: conf,
	}
}

func (w *Writer) Write(ctx context.Context, ch <-chan interface{}) error {
	var tx *sql.Tx
	if w.conf.Transactional {
		var err error
		tx, err = w.conf.DB.Begin()
		if err != nil {
			return err
		}
	}

	executable := func() error {
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

			for _, item := range items {
				var stmt *sql.Stmt
				var err error
				if w.conf.Transactional {
					stmt, err = tx.Prepare(w.conf.SQL)
					if err != nil {
						return err
					}
				} else {
					stmt, err = w.conf.DB.Prepare(w.conf.SQL)
					if err != nil {
						return err
					}
				}

				defer stmt.Close()
				args := make([]interface{}, len(item))
				for idx, e := range item {
					args[idx] = e
				}
				if _, err := stmt.Exec(args...); err != nil {
					return err
				}
			}
		}
		return nil
	}

	err := executable()
	if err != nil {
		if w.conf.Transactional {
			if errRb := tx.Rollback(); errRb != nil {
				middleware.Logger.Error(errRb)
			}
		}
		return err
	}

	if w.conf.Transactional {
		if err := tx.Commit(); err != nil {
			if errRb := tx.Rollback(); errRb != nil {
				middleware.Logger.Error(errRb)
			}
			return err
		}
	}

	return nil
}
