package zap

import (
	"context"
	"errors"
	"io"
	"time"

	caplog "github.com/nucleuskit/nucleus/cap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	logger  *zap.Logger
	closer  io.Closer
	fields  []caplog.Field
	patches []caplog.Patch
}

func New(options ...caplog.Option) (*Logger, error) {
	values := caplog.NewOptions(options...)
	config := zap.NewProductionConfig()
	if values.Level == "debug" {
		config = zap.NewDevelopmentConfig()
	}
	buildOptions := make([]zap.Option, 0, 2)
	if values.CallerSkip > 0 {
		buildOptions = append(buildOptions, zap.AddCallerSkip(values.CallerSkip))
	}
	logger, err := buildLogger(config, values, buildOptions...)
	if err != nil {
		return nil, err
	}
	if values.Service != "" {
		logger = logger.With(zap.String("service", values.Service))
	}
	return &Logger{logger: logger, closer: writerCloser(values.Writer), fields: append([]caplog.Field(nil), values.Fields...), patches: append([]caplog.Patch(nil), values.Patches...)}, nil
}

func (l *Logger) Close() error {
	if l == nil {
		return nil
	}
	var err error
	if l.logger != nil {
		err = errors.Join(err, l.logger.Sync())
	}
	if l.closer != nil {
		err = errors.Join(err, l.closer.Close())
		l.closer = nil
	}
	return err
}

func (l *Logger) Debug(ctx context.Context, message string, fields ...caplog.Field) {
	entry := caplog.NewEntry(ctx, caplog.LevelDebug, message, l.fields, fields, l.patches...)
	l.logger.Debug(entry.Message, zapFields(entry.Fields)...)
}

func (l *Logger) Info(ctx context.Context, message string, fields ...caplog.Field) {
	entry := caplog.NewEntry(ctx, caplog.LevelInfo, message, l.fields, fields, l.patches...)
	l.logger.Info(entry.Message, zapFields(entry.Fields)...)
}

func (l *Logger) Warn(ctx context.Context, message string, fields ...caplog.Field) {
	entry := caplog.NewEntry(ctx, caplog.LevelWarn, message, l.fields, fields, l.patches...)
	l.logger.Warn(entry.Message, zapFields(entry.Fields)...)
}

func (l *Logger) Error(ctx context.Context, message string, fields ...caplog.Field) {
	entry := caplog.NewEntry(ctx, caplog.LevelError, message, l.fields, fields, l.patches...)
	l.logger.Error(entry.Message, zapFields(entry.Fields)...)
}

func buildLogger(config zap.Config, values caplog.Options, options ...zap.Option) (*zap.Logger, error) {
	if values.Writer == nil {
		return config.Build(options...)
	}
	if !config.DisableCaller {
		options = append(options, zap.AddCaller())
	}
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(config.EncoderConfig),
		zapcore.AddSync(values.Writer),
		zapLevel(values.Level),
	)
	return zap.New(core, options...), nil
}

func writerCloser(writer io.Writer) io.Closer {
	closer, _ := writer.(interface {
		io.Closer
		closeOnLoggerClose()
	})
	return closer
}

func zapLevel(level caplog.Level) zapcore.LevelEnabler {
	switch level {
	case caplog.LevelDebug:
		return zapcore.DebugLevel
	case caplog.LevelWarn:
		return zapcore.WarnLevel
	case caplog.LevelError:
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

func zapFields(fields []caplog.Field) []zap.Field {
	zapFields := make([]zap.Field, 0, len(fields))
	for _, field := range fields {
		zapFields = append(zapFields, zapField(field))
	}
	return zapFields
}

func zapField(field caplog.Field) zap.Field {
	switch field.Type {
	case "string":
		if value, ok := field.Value.(string); ok {
			return zap.String(field.Key, value)
		}
	case "int":
		if value, ok := field.Value.(int); ok {
			return zap.Int(field.Key, value)
		}
	case "bool":
		if value, ok := field.Value.(bool); ok {
			return zap.Bool(field.Key, value)
		}
	case "float64":
		if value, ok := field.Value.(float64); ok {
			return zap.Float64(field.Key, value)
		}
	case "duration":
		if value, ok := field.Value.(time.Duration); ok {
			return zap.Duration(field.Key, value)
		}
	case "error":
		if value, ok := field.Value.(error); ok {
			return zap.NamedError(field.Key, value)
		}
	}
	return zap.Any(field.Key, field.Value)
}
