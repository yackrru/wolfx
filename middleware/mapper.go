package middleware

import "context"

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
