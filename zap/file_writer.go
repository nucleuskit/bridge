package zap

import (
	"os"
	"path/filepath"
)

type fileWriterOptions struct {
	flag int
	perm os.FileMode
}

type FileWriterOption func(*fileWriterOptions)

type FileWriter struct {
	*os.File
}

func WithFileTruncate() FileWriterOption {
	return func(options *fileWriterOptions) {
		options.flag = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	}
}

func NewFileWriter(path string, options ...FileWriterOption) (*FileWriter, error) {
	values := fileWriterOptions{
		flag: os.O_CREATE | os.O_WRONLY | os.O_APPEND,
		perm: 0o644,
	}
	for _, option := range options {
		if option != nil {
			option(&values)
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, values.flag, values.perm)
	if err != nil {
		return nil, err
	}
	return &FileWriter{File: file}, nil
}

func (*FileWriter) closeOnLoggerClose() {}
