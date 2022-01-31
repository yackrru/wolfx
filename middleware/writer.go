package middleware

import (
	"context"
)

// Writer receives data from channel and writes to datasource.
type Writer interface {
	Write(ctx context.Context, ch <-chan interface{}) error
}
