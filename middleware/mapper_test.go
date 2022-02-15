package middleware

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMapMapperToFlatItems(t *testing.T) {
	chunk := []MapMapperType{
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
	propsBindPosition := make(PropsBindPosition)
	propsBindPosition["created_at"] = 0
	propsBindPosition["id"] = 1
	propsBindPosition["name"] = 2

	items := MapMapperToFlatItems(chunk, propsBindPosition)
	for idx, item := range items {
		target := chunk[idx]
		assert.Equal(t, target["created_at"], item[0])
		assert.Equal(t, target["id"], item[1])
		assert.Equal(t, target["name"], item[2])
	}
}

type TestChunkType struct {
	Id   string `prop:"id"`
	Name string `prop:"name"`
}

func TestConvertItemsCustomMapper(t *testing.T) {
	chunk := []CustomMapperType{
		{
			Props: TestChunkType{
				Id:   "0",
				Name: "name0",
			},
		},
		{
			Props: TestChunkType{
				Id:   "1",
				Name: "name1",
			},
		},
		{
			Props: TestChunkType{
				Id:   "2",
				Name: "name2",
			},
		},
	}
	propsBindPosition := make(PropsBindPosition)
	propsBindPosition["id"] = 2
	propsBindPosition["name"] = 1

	items := CustomMapperToFlatItems(chunk, propsBindPosition)
	for idx, item := range items {
		target := chunk[idx]
		assert.Equal(t, target.Props.(TestChunkType).Id, item[1])
		assert.Equal(t, target.Props.(TestChunkType).Name, item[0])
	}
}
