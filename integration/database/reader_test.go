package database

import (
	"context"
	"database/sql"
	"encoding/csv"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/yackrru/wolfx/middleware"
	"io"
	"os"
	"sync"
	"testing"
)

func TestRead(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// DDL
	file, err := os.Open("./testdata/ddl.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	ddl, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(ddl)); err != nil {
		t.Fatal(err)
	}

	t.Run("MapMapperType without ChunkSize", func(t *testing.T) {
		createData(t, db)

		reader := NewReader(&ReaderConfig{
			DB:  db,
			SQL: "select * from users order by id",
		})

		var wg sync.WaitGroup
		ch := make(chan interface{})
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			if err := reader.Read(context.TODO(), ch); err != nil {
				t.Fatal(err)
			}
		}(ch)
		iChunk := <-ch
		chunk := iChunk.([]middleware.MapMapperType)

		expectedFile, err := os.Open("./testdata/expected.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer expectedFile.Close()
		csvReader := csv.NewReader(expectedFile)

		count := 0
		for {
			want, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			got := chunk[count]
			assert.Equal(t, want[0], got["id"])
			assert.Equal(t, want[1], got["name"])
			assert.Equal(t, want[2], got["joined_at"])
			count++
		}
	})

	t.Run("MapMapperType with ChunkSize", func(t *testing.T) {
		createData(t, db)

		reader := NewReader(&ReaderConfig{
			DB:        db,
			SQL:       "select * from users order by id",
			ChunkSize: 5,
		})

		var wg sync.WaitGroup
		ch := make(chan interface{})
		wg.Add(1)
		go func(ch chan interface{}) {
			defer wg.Done()
			if err := reader.Read(context.TODO(), ch); err != nil {
				t.Fatal(err)
			}
		}(ch)

		expectedFile, err := os.Open("./testdata/expected.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer expectedFile.Close()
		csvReader := csv.NewReader(expectedFile)

		count := 0
		for iChunk := range ch {
			count++

			chunk := iChunk.([]middleware.MapMapperType)

			var wants [][]string
			for i := 0; i < len(chunk); i++ {
				want, err := csvReader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				wants = append(wants, want)
			}

			for i, got := range chunk {
				want := wants[i]
				assert.Equal(t, want[0], got["id"])
				assert.Equal(t, want[1], got["name"])
				assert.Equal(t, want[2], got["joined_at"])
			}
		}
		assert.Equal(t, 5, count)
	})
}

func createData(t *testing.T, db *sql.DB) {
	if _, err := db.Exec("delete from users"); err != nil {
		t.Fatal(err)
	}

	file, err := os.Open("./testdata/create_data.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	sql, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare(string(sql))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	if _, err := stmt.Exec(); err != nil {
		t.Fatal(err)
	}
}
