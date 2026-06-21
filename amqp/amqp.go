package amqp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	caphealth "github.com/nucleuskit/cap/health"
	capmq "github.com/nucleuskit/cap/mq"
)

type Config struct {
	Exchange     string
	RoutingKey   string
	Queue        string
	Group        string
	ClientID     string
	SessionID    string
	Callback     capmq.ProducerCallback
	Retry        capmq.RetryPolicy
	DeadLetter   capmq.DeadLetterPolicy
	ErrorHandler capmq.ErrorHandler
}

var (
	ErrMissingExchange = errors.New("amqp exchange is required")
	ErrMissingQueue    = errors.New("amqp queue is required")
	ErrClosed          = errors.New("amqp broker is closed")
)

type Broker struct {
	mu           sync.Mutex
	cfg          Config
	queues       map[string][]capmq.Message
	nextOffsets  map[string]int64
	groupOffsets map[string]int64
	attempts     map[string]int
	deadLetters  map[string][]capmq.Message
	closed       bool
}

func New(cfg Config) (*Broker, error) {
	return &Broker{
		cfg:          cfg,
		queues:       map[string][]capmq.Message{},
		nextOffsets:  map[string]int64{},
		groupOffsets: map[string]int64{},
		attempts:     map[string]int{},
		deadLetters:  map[string][]capmq.Message{},
	}, nil
}

func (b *Broker) Publish(ctx context.Context, message capmq.Message) error {
	result, err := b.publish(ctx, message)
	if err != nil {
		b.notifyPublishError(ctx, message, err)
		return err
	}
	b.notifyPublishSuccess(ctx, message, result)
	return nil
}

func (b *Broker) PublishBatch(ctx context.Context, messages ...capmq.Message) ([]capmq.PublishResult, error) {
	results := make([]capmq.PublishResult, 0, len(messages))
	for _, message := range messages {
		result, err := b.publish(ctx, message)
		if err != nil {
			b.notifyPublishError(ctx, message, err)
			return nil, err
		}
		b.notifyPublishSuccess(ctx, message, result)
		results = append(results, result)
	}
	return results, nil
}

func (b *Broker) Consume(ctx context.Context) (<-chan capmq.Delivery, error) {
	return b.Subscribe(ctx, capmq.Subscription{
		Group:        b.cfg.Group,
		Topics:       topicList(b.cfg.Queue),
		ClientID:     b.cfg.ClientID,
		SessionID:    b.cfg.SessionID,
		Retry:        b.cfg.Retry,
		DeadLetter:   b.cfg.DeadLetter,
		ErrorHandler: b.cfg.ErrorHandler,
	})
}

func (b *Broker) Subscribe(ctx context.Context, subscription capmq.Subscription) (<-chan capmq.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	subscription = b.normalizeSubscription(subscription)

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrClosed
	}
	deliveries := make(chan capmq.Delivery, b.pendingMessagesLocked(subscription))
	now := time.Now()
	for _, queue := range subscription.Topics {
		messages := b.queues[queue]
		start := b.startOffsetLocked(subscription, queue, messages)
		for _, message := range messages[start:] {
			message = cloneMessage(message)
			message.Metadata.Group = subscription.Group
			message.Metadata.ClientID = subscription.ClientID
			message.Metadata.SessionID = subscription.SessionID
			if message.Metadata.ReceivedAt.IsZero() {
				message.Metadata.ReceivedAt = now
			}
			attemptKey := deliveryKey(subscription.Group, queue, message.Metadata.Offset)
			b.attempts[attemptKey]++
			message.Metadata.DeliveryAttempt = b.attempts[attemptKey]
			deliveries <- b.newDeliveryLocked(message, queue, subscription)
		}
	}
	b.mu.Unlock()

	close(deliveries)
	return deliveries, nil
}

func (b *Broker) DeadLetters(queue string) []capmq.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	if queue == "" {
		queue = b.cfg.DeadLetter.Topic
	}
	values := b.deadLetters[queue]
	messages := make([]capmq.Message, 0, len(values))
	for _, message := range values {
		messages = append(messages, cloneMessage(message))
	}
	return messages
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

func (b *Broker) ReportHealth(context.Context) (caphealth.Report, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	report := caphealth.Report{
		Capability: "mq",
		Status:     caphealth.StatusReady,
		Message:    "amqp provider ready",
		Metadata: map[string]string{
			"provider":    "amqp",
			"exchange":    b.cfg.Exchange,
			"routing_key": b.cfg.RoutingKey,
			"queue":       b.cfg.Queue,
		},
	}
	if b.closed {
		report.Status = caphealth.StatusDown
		report.Message = "amqp provider is closed"
		return report, nil
	}
	if b.cfg.Exchange == "" || b.cfg.Queue == "" {
		report.Status = caphealth.StatusDegraded
		report.Message = "amqp provider has no exchange or queue configured"
	}
	return report, nil
}

func (b *Broker) publish(ctx context.Context, message capmq.Message) (capmq.PublishResult, error) {
	if err := ctx.Err(); err != nil {
		return capmq.PublishResult{}, err
	}
	if message.Topic == "" {
		message.Topic = b.cfg.Exchange
	}
	if message.Topic == "" {
		return capmq.PublishResult{}, ErrMissingExchange
	}
	if message.Key == "" {
		message.Key = b.cfg.RoutingKey
	}
	queue := b.cfg.Queue
	if queue == "" {
		return capmq.PublishResult{}, ErrMissingQueue
	}

	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return capmq.PublishResult{}, ErrClosed
	}
	offset := b.nextOffsets[queue]
	b.nextOffsets[queue] = offset + 1
	message = cloneMessage(message)
	message.Metadata.Partition = 0
	message.Metadata.Offset = offset
	if message.Metadata.PublishedAt.IsZero() {
		message.Metadata.PublishedAt = now
	}
	if message.Metadata.Attributes == nil {
		message.Metadata.Attributes = map[string]string{}
	}
	message.Metadata.Attributes["amqp.exchange"] = message.Topic
	message.Metadata.Attributes["amqp.routing_key"] = message.Key
	message.Metadata.Attributes["amqp.queue"] = queue
	b.queues[queue] = append(b.queues[queue], cloneMessage(message))
	return capmq.PublishResult{
		MessageID: message.ID,
		Topic:     message.Topic,
		Partition: message.Metadata.Partition,
		Offset:    message.Metadata.Offset,
		Timestamp: message.Metadata.PublishedAt,
		Metadata:  message.Metadata,
	}, nil
}

func (b *Broker) normalizeSubscription(subscription capmq.Subscription) capmq.Subscription {
	if subscription.Group == "" {
		subscription.Group = b.cfg.Group
	}
	if subscription.ClientID == "" {
		subscription.ClientID = b.cfg.ClientID
	}
	if subscription.SessionID == "" {
		subscription.SessionID = b.cfg.SessionID
	}
	if len(subscription.Topics) == 0 {
		subscription.Topics = topicList(b.cfg.Queue)
	}
	if len(subscription.Topics) == 0 {
		for queue := range b.queues {
			subscription.Topics = append(subscription.Topics, queue)
		}
	}
	if subscription.Retry.MaxAttempts == 0 {
		subscription.Retry = b.cfg.Retry
	}
	if subscription.DeadLetter.Topic == "" && b.cfg.DeadLetter.Topic != "" {
		subscription.DeadLetter = b.cfg.DeadLetter
	}
	if subscription.ErrorHandler == nil {
		subscription.ErrorHandler = b.cfg.ErrorHandler
	}
	return subscription
}

func (b *Broker) pendingMessagesLocked(subscription capmq.Subscription) int {
	total := 0
	for _, queue := range subscription.Topics {
		messages := b.queues[queue]
		start := b.startOffsetLocked(subscription, queue, messages)
		if start < len(messages) {
			total += len(messages) - start
		}
	}
	return total
}

func (b *Broker) startOffsetLocked(subscription capmq.Subscription, queue string, messages []capmq.Message) int {
	key := groupQueueKey(subscription.Group, queue)
	if offset, ok := b.groupOffsets[key]; ok {
		return clampOffset(offset, len(messages))
	}
	if subscription.StartOffset == capmq.OffsetResetLatest {
		b.groupOffsets[key] = int64(len(messages))
		return len(messages)
	}
	return 0
}

func (b *Broker) newDeliveryLocked(message capmq.Message, queue string, subscription capmq.Subscription) capmq.Delivery {
	return capmq.Delivery{
		Message: cloneMessage(message),
		Ack: func(ctx context.Context) error {
			return b.decide(ctx, message, queue, subscription, capmq.Decision{Action: capmq.DecisionAck})
		},
		Nack: func(ctx context.Context, err error) error {
			return b.decide(ctx, message, queue, subscription, capmq.Decision{Action: capmq.DecisionNack, Cause: err})
		},
		Decide: func(ctx context.Context, decision capmq.Decision) error {
			return b.decide(ctx, message, queue, subscription, decision)
		},
	}
}

func (b *Broker) decide(ctx context.Context, message capmq.Message, queue string, subscription capmq.Subscription, decision capmq.Decision) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if decision.Action == "" {
		decision.Action = capmq.DecisionAck
	}
	switch decision.Action {
	case capmq.DecisionAck:
		b.commit(message.Metadata.Group, queue, message.Metadata.Offset)
	case capmq.DecisionNack, capmq.DecisionRetry:
		if subscription.ErrorHandler != nil && decision.Cause != nil {
			subscription.ErrorHandler.HandleConsumerError(ctx, decision.Cause, message.Metadata)
		}
	case capmq.DecisionDeadLetter:
		b.deadLetter(message, queue, subscription, decision)
		b.commit(message.Metadata.Group, queue, message.Metadata.Offset)
	default:
		if subscription.ErrorHandler != nil {
			subscription.ErrorHandler.HandleConsumerError(ctx, fmt.Errorf("unsupported mq decision action %q", decision.Action), message.Metadata)
		}
	}
	return nil
}

func (b *Broker) commit(group string, queue string, offset int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := groupQueueKey(group, queue)
	next := offset + 1
	if next > b.groupOffsets[key] {
		b.groupOffsets[key] = next
	}
}

func (b *Broker) deadLetter(message capmq.Message, queue string, subscription capmq.Subscription, decision capmq.Decision) {
	deadQueue := subscription.DeadLetter.Topic
	if deadQueue == "" {
		deadQueue = queue + ".dlq"
	}
	metadata := decision.DeadLetter
	if metadata.Topic == "" {
		metadata.Topic = deadQueue
	}
	if metadata.OriginalTopic == "" {
		metadata.OriginalTopic = message.Topic
	}
	if metadata.OriginalGroup == "" {
		metadata.OriginalGroup = subscription.Group
	}
	metadata.OriginalPartition = message.Metadata.Partition
	metadata.OriginalOffset = message.Metadata.Offset
	if metadata.Attempts == 0 {
		metadata.Attempts = message.Metadata.DeliveryAttempt
	}
	if metadata.Reason == "" {
		metadata.Reason = subscription.DeadLetter.Reason
	}
	if metadata.Reason == "" && decision.Cause != nil {
		metadata.Reason = decision.Cause.Error()
	}
	if metadata.FailedAt.IsZero() {
		metadata.FailedAt = time.Now()
	}
	metadata.Attributes = cloneStringMap(subscription.DeadLetter.Metadata)
	for key, value := range decision.Metadata {
		if metadata.Attributes == nil {
			metadata.Attributes = map[string]string{}
		}
		metadata.Attributes[key] = value
	}
	if metadata.Attributes == nil {
		metadata.Attributes = map[string]string{}
	}
	metadata.Attributes["amqp.queue"] = queue

	dead := cloneMessage(message)
	dead.Topic = deadQueue
	dead.Metadata.DeadLetter = &metadata
	b.mu.Lock()
	b.deadLetters[deadQueue] = append(b.deadLetters[deadQueue], dead)
	b.mu.Unlock()
}

func (b *Broker) notifyPublishSuccess(ctx context.Context, message capmq.Message, result capmq.PublishResult) {
	if b.cfg.Callback != nil {
		b.cfg.Callback.OnSuccess(ctx, message, result)
	}
}

func (b *Broker) notifyPublishError(ctx context.Context, message capmq.Message, err error) {
	if b.cfg.Callback != nil {
		b.cfg.Callback.OnError(ctx, message, err)
	}
}

func cloneMessage(message capmq.Message) capmq.Message {
	copied := message
	copied.Body = append([]byte(nil), message.Body...)
	copied.Headers = cloneStringMap(message.Headers)
	copied.Header = cloneStringMap(message.Header)
	copied.Metadata.Attributes = cloneStringMap(message.Metadata.Attributes)
	if message.Metadata.DeadLetter != nil {
		deadLetter := *message.Metadata.DeadLetter
		deadLetter.Attributes = cloneStringMap(message.Metadata.DeadLetter.Attributes)
		copied.Metadata.DeadLetter = &deadLetter
	}
	return copied
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	copied := make(map[string]string, len(values))
	for key, value := range values {
		copied[key] = value
	}
	return copied
}

func groupQueueKey(group string, queue string) string {
	return group + "\x00" + queue
}

func deliveryKey(group string, queue string, offset int64) string {
	return fmt.Sprintf("%s\x00%s\x00%d", group, queue, offset)
}

func topicList(topic string) []string {
	if topic == "" {
		return nil
	}
	return []string{topic}
}

func clampOffset(offset int64, size int) int {
	if offset <= 0 {
		return 0
	}
	if offset >= int64(size) {
		return size
	}
	return int(offset)
}
