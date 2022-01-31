package middleware

import "context"

// Reader reads data from datasource and sends to channel.
type Reader interface {
	Read(ctx context.Context, ch chan<- interface{}) error
}
