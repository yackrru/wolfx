package middleware

import (
	"context"
	"sort"
)

const CustomMapperTag = "prop"

// MapMapperType is the default sending data type.
type MapMapperType map[string]string

// CustomMapperType is the custom sending data type.
type CustomMapperType struct {

	// Props expects struct type.
	// Each element of the Props struct
	// is given a prop tag.
	// For the prop value, enter the corresponding header name.
	// Example: `prop:"id"`
	Props interface{}
}

// RowMapper is the function type to map csv rows to user's own struct.
type RowMapper func(ctx context.Context, ch chan<- interface{}, chunk []MapMapperType) error

// PropsBindPosition is the output position binding.
// Keys are prop (column) names and values are positions starting from 0.
type PropsBindPosition map[string]uint

// MapMapperToFlatItems converts chunk of MapMapperType to slices
// sorted by output element order.
func MapMapperToFlatItems(chunk []MapMapperType,
	propsBindPosition PropsBindPosition) [][]string {

	var items [][]string
	for _, itemBuf := range chunk {
		itemMap := make(map[int]string)
		for k, v := range itemBuf {
			position := int(propsBindPosition[k])
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
