package zap

import (
	"errors"
	"io"
	"sync"
)

const defaultAsyncBufferSize = 256

var ErrAsyncWriterClosed = errors.New("zap async writer closed")

type asyncWriterOptions struct {
	bufferSize int
}

type AsyncWriterOption func(*asyncWriterOptions)

func WithAsyncBufferSize(size int) AsyncWriterOption {
	return func(options *asyncWriterOptions) {
		if size > 0 {
			options.bufferSize = size
		}
	}
}

type AsyncWriter struct {
	writer io.Writer
	closer io.Closer
	syncer interface {
		Sync() error
	}

	stateMu sync.Mutex
	writeMu sync.Mutex
	jobs    chan []byte
	done    chan struct{}
	closed  bool
	err     error

	closeOnce sync.Once
	closeErr  error
}

func NewAsyncWriter(writer io.Writer, options ...AsyncWriterOption) (*AsyncWriter, error) {
	if writer == nil {
		return nil, errors.New("zap async writer requires writer")
	}
	values := asyncWriterOptions{bufferSize: defaultAsyncBufferSize}
	for _, option := range options {
		if option != nil {
			option(&values)
		}
	}
	async := &AsyncWriter{
		writer: writer,
		closer: writerCloser(writer),
		jobs:   make(chan []byte, values.bufferSize),
		done:   make(chan struct{}),
	}
	async.syncer, _ = writer.(interface{ Sync() error })
	go async.run()
	return async, nil
}

func (*AsyncWriter) closeOnLoggerClose() {}

func (w *AsyncWriter) Write(p []byte) (int, error) {
	if w == nil {
		return 0, ErrAsyncWriterClosed
	}
	value := append([]byte(nil), p...)
	w.stateMu.Lock()
	if w.closed {
		w.stateMu.Unlock()
		return 0, ErrAsyncWriterClosed
	}
	priorErr := w.err
	select {
	case w.jobs <- value:
		w.stateMu.Unlock()
		return len(p), priorErr
	default:
		w.stateMu.Unlock()
		err := w.write(value)
		if err != nil {
			w.recordError(err)
		}
		return len(p), errors.Join(priorErr, err)
	}
}

func (w *AsyncWriter) Close() error {
	if w == nil {
		return nil
	}
	w.closeOnce.Do(func() {
		w.stateMu.Lock()
		w.closed = true
		close(w.jobs)
		w.stateMu.Unlock()
		<-w.done
		w.writeMu.Lock()
		if w.syncer != nil {
			w.recordError(w.syncer.Sync())
		}
		if w.closer != nil {
			w.recordError(w.closer.Close())
		}
		w.writeMu.Unlock()
		w.closeErr = w.Err()
	})
	return w.closeErr
}

func (w *AsyncWriter) Err() error {
	if w == nil {
		return nil
	}
	w.stateMu.Lock()
	defer w.stateMu.Unlock()
	return w.err
}

func (w *AsyncWriter) run() {
	for value := range w.jobs {
		if err := w.write(value); err != nil {
			w.recordError(err)
		}
	}
	close(w.done)
}

func (w *AsyncWriter) write(value []byte) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	n, err := w.writer.Write(value)
	if err == nil && n < len(value) {
		err = io.ErrShortWrite
	}
	return err
}

func (w *AsyncWriter) recordError(err error) {
	if err == nil {
		return
	}
	w.stateMu.Lock()
	w.err = errors.Join(w.err, err)
	w.stateMu.Unlock()
}
