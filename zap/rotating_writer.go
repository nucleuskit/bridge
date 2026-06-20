package zap

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	defaultRotationMaxBytes   int64 = 10 << 20
	defaultRotationMaxBackups       = 5
)

type rotationOptions struct {
	maxBytes   int64
	maxBackups int
}

type RotationOption func(*rotationOptions)

type RotatingFileWriter struct {
	path       string
	maxBytes   int64
	maxBackups int

	mu     sync.Mutex
	file   *os.File
	size   int64
	closed bool
}

func WithRotationMaxBytes(size int64) RotationOption {
	return func(options *rotationOptions) {
		if size > 0 {
			options.maxBytes = size
		}
	}
}

func WithRotationMaxBackups(count int) RotationOption {
	return func(options *rotationOptions) {
		if count >= 0 {
			options.maxBackups = count
		}
	}
}

func NewRotatingFileWriter(path string, options ...RotationOption) (*RotatingFileWriter, error) {
	values := rotationOptions{
		maxBytes:   defaultRotationMaxBytes,
		maxBackups: defaultRotationMaxBackups,
	}
	for _, option := range options {
		if option != nil {
			option(&values)
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &RotatingFileWriter{
		path:       path,
		maxBytes:   values.maxBytes,
		maxBackups: values.maxBackups,
		file:       file,
		size:       stat.Size(),
	}, nil
}

func (*RotatingFileWriter) closeOnLoggerClose() {}

func (w *RotatingFileWriter) Write(p []byte) (int, error) {
	if w == nil {
		return 0, os.ErrClosed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, os.ErrClosed
	}
	if w.size > 0 && w.size+int64(len(p)) > w.maxBytes {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	if err == nil && n < len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

func (w *RotatingFileWriter) Sync() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	return w.file.Sync()
}

func (w *RotatingFileWriter) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *RotatingFileWriter) rotateLocked() error {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
		w.file = nil
	}
	if w.maxBackups <= 0 {
		if err := os.Remove(w.path); err != nil && !os.IsNotExist(err) {
			return err
		}
	} else {
		last := backupPath(w.path, w.maxBackups)
		if err := os.Remove(last); err != nil && !os.IsNotExist(err) {
			return err
		}
		for i := w.maxBackups - 1; i >= 1; i-- {
			oldPath := backupPath(w.path, i)
			newPath := backupPath(w.path, i+1)
			if err := os.Rename(oldPath, newPath); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		if err := os.Rename(w.path, backupPath(w.path, 1)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w.file = file
	w.size = 0
	return nil
}

func backupPath(path string, index int) string {
	return path + "." + strconv.Itoa(index)
}
