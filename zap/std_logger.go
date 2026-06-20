package zap

import (
	"io"
	stdlog "log"
	"sync"
)

var standardLoggerMu sync.Mutex

func RedirectStandardLogger(writer io.Writer, flags int) func() {
	if writer == nil {
		writer = io.Discard
	}
	standardLoggerMu.Lock()
	previousWriter := stdlog.Writer()
	previousFlags := stdlog.Flags()
	previousPrefix := stdlog.Prefix()
	stdlog.SetOutput(writer)
	stdlog.SetFlags(flags)

	var once sync.Once
	return func() {
		once.Do(func() {
			stdlog.SetOutput(previousWriter)
			stdlog.SetFlags(previousFlags)
			stdlog.SetPrefix(previousPrefix)
			standardLoggerMu.Unlock()
		})
	}
}
