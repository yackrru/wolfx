package database

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/assert"
	"github.com/yackrru/wolfx/middleware"
	"sync"
	"testing"
)

func TestWriteStandard(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// DDL
	if _, err := db.Exec("create table users (id integer, name text)"); err != nil {
		t.Fatal(err)
	}

	propsBindPosition := make(middleware.PropsBindPosition)
	propsBindPosition["id"] = 0
	propsBindPosition["name"] = 1

	t.Run("Insert without transaction", func(t *testing.T) {
		conf := &WriterConfig{
			DB:                db,
			SQL:               "insert into users values (?, ?)",
			PropsBindPosition: propsBindPosition,
		}
		execWriteStandardTest(t, db, conf)
	})

	t.Run("Insert with transaction", func(t *testing.T) {
		if _, err := db.Exec("delete from users"); err != nil {
			t.Fatal(err)
		}
		conf := &WriterConfig{
			DB:                db,
			SQL:               "insert into users values (?, ?)",
			Transactional:     true,
			PropsBindPosition: propsBindPosition,
		}
		execWriteStandardTest(t, db, conf)
	})
}

func execWriteStandardTest(t *testing.T, db *sql.DB, conf *WriterConfig) {
	writer := NewWriter(conf)

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
		{"id": "2", "name": "name2"},
	}
	ch <- chunk
	close(ch)
	wg.Wait()

	rows, err := db.Query("select * from users order by id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id string
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, chunk[count]["id"], id)
		assert.Equal(t, chunk[count]["name"], name)
		count++
	}
}
