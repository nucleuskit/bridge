package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	caphealth "github.com/nucleuskit/cap/health"
	capmq "github.com/nucleuskit/cap/mq"
)

type Config struct {
	Servers      []string
	Subject      string
	Group        string
	ClientID     string
	SessionID    string
	Callback     capmq.ProducerCallback
	Retry        capmq.RetryPolicy
	DeadLetter   capmq.DeadLetterPolicy
	ErrorHandler capmq.ErrorHandler
}

var (
	ErrMissingSubject = errors.New("nats subject is required")
	ErrClosed         = errors.New("nats broker is closed")
)

type Broker struct {
	mu           sync.Mutex
	cfg          Config
	subjects     map[string][]capmq.Message
	nextSequence map[string]int64
	groupCursor  map[string]int64
	attempts     map[string]int
	deadLetters  map[string][]capmq.Message
	closed       bool
}

func New(cfg Config) (*Broker, error) {
	return &Broker{
		cfg:          cfg,
		subjects:     map[string][]capmq.Message{},
		nextSequence: map[string]int64{},
		groupCursor:  map[string]int64{},
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
		Topics:       subjectList(b.cfg.Subject),
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
	for _, subject := range subscription.Topics {
		queue := b.subjects[subject]
		start := b.startCursorLocked(subscription, subject, queue)
		for _, message := range queue[start:] {
			message = cloneMessage(message)
			message.Metadata.Group = subscription.Group
			message.Metadata.ClientID = subscription.ClientID
			message.Metadata.SessionID = subscription.SessionID
			if message.Metadata.ReceivedAt.IsZero() {
				message.Metadata.ReceivedAt = now
			}
			attemptKey := deliveryKey(subscription.Group, message.Topic, message.Metadata.Offset)
			b.attempts[attemptKey]++
			message.Metadata.DeliveryAttempt = b.attempts[attemptKey]
			deliveries <- b.newDeliveryLocked(message, subscription)
		}
	}
	b.mu.Unlock()

	close(deliveries)
	return deliveries, nil
}

func (b *Broker) DeadLetters(subject string) []capmq.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	if subject == "" {
		subject = b.cfg.DeadLetter.Topic
	}
	values := b.deadLetters[subject]
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
		Message:    "nats provider ready",
		Metadata: map[string]string{
			"provider":  "nats",
			"servers":   strconv.Itoa(len(b.cfg.Servers)),
			"subject":   b.cfg.Subject,
			"group":     b.cfg.Group,
			"client_id": b.cfg.ClientID,
		},
	}
	if b.closed {
		report.Status = caphealth.StatusDown
		report.Message = "nats provider is closed"
		return report, nil
	}
	if b.cfg.Subject == "" && len(b.cfg.Servers) == 0 {
		report.Status = caphealth.StatusDegraded
		report.Message = "nats provider has no subject or servers configured"
	}
	return report, nil
}

func (b *Broker) publish(ctx context.Context, message capmq.Message) (capmq.PublishResult, error) {
	if err := ctx.Err(); err != nil {
		return capmq.PublishResult{}, err
	}
	if message.Topic == "" {
		message.Topic = b.cfg.Subject
	}
	if message.Topic == "" {
		return capmq.PublishResult{}, ErrMissingSubject
	}

	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return capmq.PublishResult{}, ErrClosed
	}
	sequence := b.nextSequence[message.Topic]
	b.nextSequence[message.Topic] = sequence + 1
	message = cloneMessage(message)
	message.Metadata.Partition = 0
	message.Metadata.Offset = sequence
	if message.Metadata.PublishedAt.IsZero() {
		message.Metadata.PublishedAt = now
	}
	b.subjects[message.Topic] = append(b.subjects[message.Topic], cloneMessage(message))
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
		subscription.Topics = subjectList(b.cfg.Subject)
	}
	if len(subscription.Topics) == 0 {
		for subject := range b.subjects {
			subscription.Topics = append(subscription.Topics, subject)
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
	for _, subject := range subscription.Topics {
		queue := b.subjects[subject]
		start := b.startCursorLocked(subscription, subject, queue)
		if start < len(queue) {
			total += len(queue) - start
		}
	}
	return total
}

func (b *Broker) startCursorLocked(subscription capmq.Subscription, subject string, queue []capmq.Message) int {
	key := groupSubjectKey(subscription.Group, subject)
	if cursor, ok := b.groupCursor[key]; ok {
		return clampCursor(cursor, len(queue))
	}
	if subscription.StartOffset == capmq.OffsetResetLatest {
		b.groupCursor[key] = int64(len(queue))
		return len(queue)
	}
	return 0
}

func (b *Broker) newDeliveryLocked(message capmq.Message, subscription capmq.Subscription) capmq.Delivery {
	return capmq.Delivery{
		Message: cloneMessage(message),
		Ack: func(ctx context.Context) error {
			return b.decide(ctx, message, subscription, capmq.Decision{Action: capmq.DecisionAck})
		},
		Nack: func(ctx context.Context, err error) error {
			return b.decide(ctx, message, subscription, capmq.Decision{Action: capmq.DecisionNack, Cause: err})
		},
		Decide: func(ctx context.Context, decision capmq.Decision) error {
			return b.decide(ctx, message, subscription, decision)
		},
	}
}

func (b *Broker) decide(ctx context.Context, message capmq.Message, subscription capmq.Subscription, decision capmq.Decision) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if decision.Action == "" {
		decision.Action = capmq.DecisionAck
	}
	switch decision.Action {
	case capmq.DecisionAck:
		b.commit(message.Metadata.Group, message.Topic, message.Metadata.Offset)
	case capmq.DecisionNack, capmq.DecisionRetry:
		if subscription.ErrorHandler != nil && decision.Cause != nil {
			subscription.ErrorHandler.HandleConsumerError(ctx, decision.Cause, message.Metadata)
		}
	case capmq.DecisionDeadLetter:
		b.deadLetter(message, subscription, decision)
		b.commit(message.Metadata.Group, message.Topic, message.Metadata.Offset)
	default:
		if subscription.ErrorHandler != nil {
			subscription.ErrorHandler.HandleConsumerError(ctx, fmt.Errorf("unsupported mq decision action %q", decision.Action), message.Metadata)
		}
	}
	return nil
}

func (b *Broker) commit(group string, subject string, sequence int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := groupSubjectKey(group, subject)
	next := sequence + 1
	if next > b.groupCursor[key] {
		b.groupCursor[key] = next
	}
}

func (b *Broker) deadLetter(message capmq.Message, subscription capmq.Subscription, decision capmq.Decision) {
	subject := subscription.DeadLetter.Topic
	if subject == "" {
		subject = message.Topic + ".DLQ"
	}
	metadata := decision.DeadLetter
	if metadata.Topic == "" {
		metadata.Topic = subject
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

	dead := cloneMessage(message)
	dead.Topic = subject
	dead.Metadata.DeadLetter = &metadata
	b.mu.Lock()
	b.deadLetters[subject] = append(b.deadLetters[subject], dead)
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

func groupSubjectKey(group string, subject string) string {
	return group + "\x00" + subject
}

func deliveryKey(group string, subject string, sequence int64) string {
	return fmt.Sprintf("%s\x00%s\x00%d", group, subject, sequence)
}

func subjectList(subject string) []string {
	if subject == "" {
		return nil
	}
	return []string{subject}
}

func clampCursor(cursor int64, size int) int {
	if cursor <= 0 {
		return 0
	}
	if cursor >= int64(size) {
		return size
	}
	return int(cursor)
}
