package prometheus

import (
	"context"
	"fmt"
	"strings"
	"time"

	capmetric "github.com/nucleuskit/nucleus/cap/metric"
	capmq "github.com/nucleuskit/nucleus/cap/mq"
	capredis "github.com/nucleuskit/nucleus/cap/redis"
	capsql "github.com/nucleuskit/nucleus/cap/sql"
)

type adapterStartKey struct{}

func SQLHook(meter capmetric.Meter) capsql.QueryHook {
	return sqlMetrics{meter: meter}
}

func RedisHook(meter capmetric.Meter) capredis.OperationHook {
	return redisMetrics{meter: meter}
}

func MQObserver(meter capmetric.Meter) *MQMetrics {
	return &MQMetrics{meter: meter}
}

type sqlMetrics struct {
	meter capmetric.Meter
}

type redisMetrics struct {
	meter capmetric.Meter
}

type MQMetrics struct {
	meter capmetric.Meter
}

func (m sqlMetrics) BeforeQuery(ctx context.Context, metadata capsql.QueryMetadata) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, adapterStartKey{}, time.Now())
}

func (m sqlMetrics) AfterQuery(ctx context.Context, metadata capsql.QueryMetadata) {
	if m.meter == nil {
		return
	}
	status := statusLabel(metadata.Err)
	labels := []capmetric.Attribute{
		capmetric.String("db", metadata.Name),
		capmetric.String("driver", metadata.Driver),
		capmetric.String("operation", strings.ToLower(metadata.Operation)),
		capmetric.String("target", metadata.Target),
		capmetric.String("status", status),
	}
	m.meter.Counter("sql_queries_total", capmetric.WithLabels("db", "driver", "operation", "target", "status")).Add(ctx, 1, labels...)
	duration := metadata.Duration
	if duration == 0 && !metadata.StartedAt.IsZero() {
		duration = time.Since(metadata.StartedAt)
	}
	if duration == 0 {
		duration = durationFromContext(ctx)
	}
	if duration > 0 {
		m.meter.Histogram("sql_query_duration_seconds", capmetric.WithLabels("db", "driver", "operation", "target", "status")).
			Observe(ctx, duration.Seconds(), labels...)
	}
}

func (m redisMetrics) BeforeRedis(ctx context.Context, event capredis.OperationEvent) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, adapterStartKey{}, time.Now())
}

func (m redisMetrics) AfterRedis(ctx context.Context, event capredis.OperationEvent) {
	if m.meter == nil {
		return
	}
	commandCount := event.CommandCount
	if commandCount <= 0 {
		commandCount = 1
	}
	command := strings.ToUpper(event.Name)
	status := statusLabel(event.Err)
	labels := []capmetric.Attribute{
		capmetric.String("command", command),
		capmetric.String("status", status),
	}
	m.meter.Counter("redis_commands_total", capmetric.WithLabels("command", "status")).Add(ctx, float64(commandCount), labels...)
	duration := event.Duration
	if duration == 0 && !event.StartedAt.IsZero() {
		duration = time.Since(event.StartedAt)
	}
	if duration == 0 {
		duration = durationFromContext(ctx)
	}
	if duration > 0 {
		m.meter.Histogram("redis_command_duration_seconds", capmetric.WithLabels("command", "status")).
			Observe(ctx, duration.Seconds(), labels...)
	}
}

func (m *MQMetrics) ProducerCallback() capmq.ProducerCallback {
	return capmq.ProducerCallbackFunc{
		Success: func(ctx context.Context, message capmq.Message, result capmq.PublishResult) {
			if result.Topic != "" {
				message.Topic = result.Topic
			}
			message.Metadata.Partition = result.Partition
			m.recordMessage(ctx, "publish", message, nil)
		},
		Error: func(ctx context.Context, message capmq.Message, err error) {
			m.recordMessage(ctx, "publish", message, err)
		},
	}
}

func (m *MQMetrics) ErrorHandler() capmq.ErrorHandler {
	return capmq.ErrorHandlerFunc(func(ctx context.Context, err error, metadata capmq.Metadata) {
		if m == nil || m.meter == nil {
			return
		}
		m.meter.Counter("mq_consumer_errors_total", capmetric.WithLabels("topic", "group")).
			Add(ctx, 1, capmetric.String("topic", ""), capmetric.String("group", metadata.Group))
	})
}

func (m *MQMetrics) RecordDelivery(ctx context.Context, message capmq.Message) {
	if m == nil || m.meter == nil {
		return
	}
	m.recordMessage(ctx, "delivery", message, nil)
	if message.Metadata.DeliveryAttempt > 0 {
		m.meter.Gauge("mq_delivery_attempts", capmetric.WithLabels("topic", "group", "partition")).
			Set(ctx, float64(message.Metadata.DeliveryAttempt),
				capmetric.String("topic", message.Topic),
				capmetric.String("group", message.Metadata.Group),
				capmetric.String("partition", fmt.Sprint(message.Metadata.Partition)),
			)
	}
}

func (m *MQMetrics) RecordDecision(ctx context.Context, message capmq.Message, decision capmq.Decision, err error) {
	operation := string(decision.Action)
	if operation == "" {
		operation = "ack"
	}
	m.recordMessage(ctx, operation, message, err)
}

func (m *MQMetrics) recordMessage(ctx context.Context, operation string, message capmq.Message, err error) {
	if m == nil || m.meter == nil {
		return
	}
	m.meter.Counter("mq_messages_total", capmetric.WithLabels("topic", "group", "operation", "status")).
		Add(ctx, 1,
			capmetric.String("topic", message.Topic),
			capmetric.String("group", message.Metadata.Group),
			capmetric.String("operation", operation),
			capmetric.String("status", statusLabel(err)),
		)
}

func statusLabel(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

func durationFromContext(ctx context.Context) time.Duration {
	if ctx == nil {
		return 0
	}
	startedAt, _ := ctx.Value(adapterStartKey{}).(time.Time)
	if startedAt.IsZero() {
		return 0
	}
	return time.Since(startedAt)
}
