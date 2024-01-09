package closer

import (
	"sync"
)

// Closer is the interface for object that can release its resources.
type Closer interface {
	// Close release all resources holded by the object.
	Close()
	// Done returns a channel that's closed when object was closed.
	Done() <-chan struct{}
}

// Close release all resources holded by the object.
func Close(obj interface{}) {
	if obj == nil {
		return
	}
	if c, ok := obj.(Closer); ok {
		c.Close()
	}
}

// New returns a closer.
func New() Closer {
	return &closer{ch: make(chan struct{})}
}

type closer struct {
	once sync.Once
	ch   chan struct{}
}

func (c *closer) Close() {
	c.once.Do(func() {
		close(c.ch)
	})
}

func (c *closer) Done() <-chan struct{} {
	return c.ch
}
