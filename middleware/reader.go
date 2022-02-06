package middleware

import "context"

// Reader reads data from datasource and sends to channel.
type Reader interface {

	// Read should close the channel at the end of the process
	// or call defer close(channel).
	Read(ctx context.Context, ch chan<- interface{}) error
}
