package sarama

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	ibmsarama "github.com/IBM/sarama"
	caphealth "github.com/nucleuskit/nucleus/cap/health"
	capmq "github.com/nucleuskit/nucleus/cap/mq"
)

type Config struct {
	Brokers          []string
	Topic            string
	Group            string
	ClientID         string
	Callback         capmq.ProducerCallback
	ErrorHandler     capmq.ErrorHandler
	Producer         ibmsarama.SyncProducer
	ConsumerGroup    ibmsarama.ConsumerGroup
	SaramaConfig     *ibmsarama.Config
	Observer         Observer
	RebalanceHandler RebalanceHandler
}

type Broker struct {
	cfg           Config
	producer      ibmsarama.SyncProducer
	consumerGroup ibmsarama.ConsumerGroup
	closed        bool
}

type EventKind string

const (
	EventPublishSuccess EventKind = "publish_success"
	EventPublishError   EventKind = "publish_error"
	EventDelivery       EventKind = "delivery"
	EventOffsetCommit   EventKind = "offset_commit"
	EventOffsetReset    EventKind = "offset_reset"
	EventDeadLetter     EventKind = "dead_letter"
	EventConsumerError  EventKind = "consumer_error"
	EventRebalanceStart EventKind = "rebalance_start"
	EventRebalanceEnd   EventKind = "rebalance_end"
)

type BrokerEvent struct {
	Kind      EventKind
	Topic     string
	Group     string
	Partition int
	Offset    int64
	MemberID  string
	Err       error
	Time      time.Time
	Metadata  map[string]string
}

type Observer interface {
	ObserveBrokerEvent(context.Context, BrokerEvent)
}

type RebalanceHandler interface {
	OnRebalanceStart(context.Context, Assignment)
	OnRebalanceEnd(context.Context, Assignment)
}

type Assignment struct {
	Group        string
	MemberID     string
	GenerationID int32
	Claims       map[string][]int32
}

var (
	ErrMissingTopic         = errors.New("sarama topic is required")
	ErrMissingProducer      = errors.New("sarama producer is required")
	ErrMissingConsumerGroup = errors.New("sarama consumer group is required")
)

func New(cfg Config) (*Broker, error) {
	saramaConfig := cfg.SaramaConfig
	if saramaConfig == nil {
		saramaConfig = ibmsarama.NewConfig()
	}
	if cfg.ClientID != "" {
		saramaConfig.ClientID = cfg.ClientID
	}
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Consumer.Return.Errors = true

	injectedProducer := cfg.Producer != nil
	injectedConsumerGroup := cfg.ConsumerGroup != nil

	producer := cfg.Producer
	if producer == nil && !injectedConsumerGroup && len(cfg.Brokers) > 0 {
		created, err := ibmsarama.NewSyncProducer(cfg.Brokers, saramaConfig)
		if err != nil {
			return nil, err
		}
		producer = created
	}

	consumerGroup := cfg.ConsumerGroup
	if consumerGroup == nil && !injectedProducer && len(cfg.Brokers) > 0 && cfg.Group != "" {
		created, err := ibmsarama.NewConsumerGroup(cfg.Brokers, cfg.Group, saramaConfig)
		if err != nil {
			if producer != nil && cfg.Producer == nil {
				_ = producer.Close()
			}
			return nil, err
		}
		consumerGroup = created
	}

	return &Broker{
		cfg:           cfg,
		producer:      producer,
		consumerGroup: consumerGroup,
	}, nil
}

func (b *Broker) Publish(ctx context.Context, message capmq.Message) error {
	result, err := b.send(ctx, message)
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
		result, err := b.send(ctx, message)
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
		Group:    b.cfg.Group,
		Topics:   topicList(b.cfg.Topic),
		ClientID: b.cfg.ClientID,
	})
}

func (b *Broker) Subscribe(ctx context.Context, subscription capmq.Subscription) (<-chan capmq.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.consumerGroup == nil {
		return nil, ErrMissingConsumerGroup
	}
	subscription = b.normalizeSubscription(subscription)
	deliveries := make(chan capmq.Delivery, maxInFlight(subscription.MaxInFlight))
	handler := &consumerGroupHandler{
		broker:       b,
		subscription: subscription,
		deliveries:   deliveries,
	}
	go func() {
		defer close(deliveries)
		if err := b.consumerGroup.Consume(ctx, subscription.Topics, handler); err != nil {
			b.handleConsumerError(ctx, err, capmq.Metadata{Group: subscription.Group})
		}
	}()
	return deliveries, nil
}

func (b *Broker) PollErrors(ctx context.Context) {
	if b.consumerGroup == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-b.consumerGroup.Errors():
			if !ok {
				return
			}
			b.handleConsumerError(ctx, err, capmq.Metadata{Group: b.cfg.Group})
		default:
			return
		}
	}
}

func (b *Broker) Close() error {
	b.closed = true
	var err error
	if b.consumerGroup != nil {
		err = errors.Join(err, b.consumerGroup.Close())
	}
	if b.producer != nil {
		err = errors.Join(err, b.producer.Close())
	}
	return err
}

func (b *Broker) ReportHealth(context.Context) (caphealth.Report, error) {
	report := caphealth.Report{
		Capability: "mq",
		Status:     caphealth.StatusReady,
		Message:    "sarama provider ready",
		Metadata: map[string]string{
			"provider": "sarama",
			"brokers":  strconv.Itoa(len(b.cfg.Brokers)),
			"topic":    b.cfg.Topic,
			"group":    b.cfg.Group,
		},
	}
	if b.closed {
		report.Status = caphealth.StatusDown
		report.Message = "sarama provider is closed"
		return report, nil
	}
	if b.producer == nil && b.consumerGroup == nil {
		report.Status = caphealth.StatusDegraded
		report.Message = "sarama provider has no producer or consumer group"
	}
	return report, nil
}

func (b *Broker) send(ctx context.Context, message capmq.Message) (capmq.PublishResult, error) {
	if err := ctx.Err(); err != nil {
		return capmq.PublishResult{}, err
	}
	if b.producer == nil {
		return capmq.PublishResult{}, ErrMissingProducer
	}
	if message.Topic == "" {
		message.Topic = b.cfg.Topic
	}
	if message.Topic == "" {
		return capmq.PublishResult{}, ErrMissingTopic
	}
	producerMessage := toProducerMessage(message)
	partition, offset, err := b.producer.SendMessage(producerMessage)
	if err != nil {
		return capmq.PublishResult{}, err
	}
	timestamp := producerMessage.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	return capmq.PublishResult{
		MessageID: message.ID,
		Topic:     message.Topic,
		Partition: int(partition),
		Offset:    offset,
		Timestamp: timestamp,
		Metadata: capmq.Metadata{
			Partition:   int(partition),
			Offset:      offset,
			PublishedAt: timestamp,
		},
	}, nil
}

func (b *Broker) normalizeSubscription(subscription capmq.Subscription) capmq.Subscription {
	if subscription.Group == "" {
		subscription.Group = b.cfg.Group
	}
	if subscription.ClientID == "" {
		subscription.ClientID = b.cfg.ClientID
	}
	if len(subscription.Topics) == 0 {
		subscription.Topics = topicList(b.cfg.Topic)
	}
	if subscription.ErrorHandler == nil {
		subscription.ErrorHandler = b.cfg.ErrorHandler
	}
	return subscription
}

func (b *Broker) notifyPublishSuccess(ctx context.Context, message capmq.Message, result capmq.PublishResult) {
	if b.cfg.Callback != nil {
		b.cfg.Callback.OnSuccess(ctx, message, result)
	}
	b.observe(ctx, BrokerEvent{
		Kind:      EventPublishSuccess,
		Topic:     result.Topic,
		Group:     b.cfg.Group,
		Partition: result.Partition,
		Offset:    result.Offset,
	})
}

func (b *Broker) notifyPublishError(ctx context.Context, message capmq.Message, err error) {
	if b.cfg.Callback != nil {
		b.cfg.Callback.OnError(ctx, message, err)
	}
	b.observe(ctx, BrokerEvent{Kind: EventPublishError, Topic: message.Topic, Group: b.cfg.Group, Err: err})
}

func (b *Broker) handleConsumerError(ctx context.Context, err error, metadata capmq.Metadata) {
	if b.cfg.ErrorHandler != nil {
		b.cfg.ErrorHandler.HandleConsumerError(ctx, err, metadata)
	}
	b.observe(ctx, BrokerEvent{
		Kind:      EventConsumerError,
		Topic:     metadata.Attributes["topic"],
		Group:     metadata.Group,
		Partition: metadata.Partition,
		Offset:    metadata.Offset,
		MemberID:  metadata.MemberID,
		Err:       err,
	})
}

func (b *Broker) observe(ctx context.Context, event BrokerEvent) {
	if b.cfg.Observer == nil {
		return
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	b.cfg.Observer.ObserveBrokerEvent(ctx, event)
}

type consumerGroupHandler struct {
	broker       *Broker
	subscription capmq.Subscription
	deliveries   chan<- capmq.Delivery
}

func (h *consumerGroupHandler) Setup(session ibmsarama.ConsumerGroupSession) error {
	assignment := h.assignment(session)
	if h.broker.cfg.RebalanceHandler != nil {
		h.broker.cfg.RebalanceHandler.OnRebalanceStart(session.Context(), assignment)
	}
	h.broker.observe(session.Context(), BrokerEvent{
		Kind:     EventRebalanceStart,
		Group:    h.subscription.Group,
		MemberID: session.MemberID(),
		Metadata: map[string]string{"generation_id": strconv.FormatInt(int64(session.GenerationID()), 10)},
	})
	return nil
}

func (h *consumerGroupHandler) Cleanup(session ibmsarama.ConsumerGroupSession) error {
	assignment := h.assignment(session)
	if h.broker.cfg.RebalanceHandler != nil {
		h.broker.cfg.RebalanceHandler.OnRebalanceEnd(session.Context(), assignment)
	}
	h.broker.observe(session.Context(), BrokerEvent{
		Kind:     EventRebalanceEnd,
		Group:    h.subscription.Group,
		MemberID: session.MemberID(),
		Metadata: map[string]string{"generation_id": strconv.FormatInt(int64(session.GenerationID()), 10)},
	})
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session ibmsarama.ConsumerGroupSession, claim ibmsarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return session.Context().Err()
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			delivery := h.delivery(session, message)
			h.broker.observe(session.Context(), BrokerEvent{
				Kind:      EventDelivery,
				Topic:     message.Topic,
				Group:     h.subscription.Group,
				Partition: int(message.Partition),
				Offset:    message.Offset,
				MemberID:  session.MemberID(),
			})
			select {
			case <-session.Context().Done():
				return session.Context().Err()
			case h.deliveries <- delivery:
			}
		}
	}
}

func (h *consumerGroupHandler) delivery(session ibmsarama.ConsumerGroupSession, saramaMessage *ibmsarama.ConsumerMessage) capmq.Delivery {
	message := fromConsumerMessage(saramaMessage, h.subscription, session)
	return capmq.Delivery{
		Message: message,
		Decide: func(ctx context.Context, decision capmq.Decision) error {
			return h.decide(ctx, session, saramaMessage, message.Metadata, decision)
		},
	}
}

func (h *consumerGroupHandler) decide(ctx context.Context, session ibmsarama.ConsumerGroupSession, message *ibmsarama.ConsumerMessage, metadata capmq.Metadata, decision capmq.Decision) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if decision.Action == "" {
		decision.Action = capmq.DecisionAck
	}
	switch decision.Action {
	case capmq.DecisionAck:
		session.MarkOffset(message.Topic, message.Partition, message.Offset+1, offsetMetadata(decision))
		session.Commit()
		h.broker.observe(ctx, eventFromMetadata(EventOffsetCommit, metadata, nil))
	case capmq.DecisionNack:
		if decision.Cause != nil {
			h.handleDeliveryError(ctx, decision.Cause, metadata)
		}
	case capmq.DecisionRetry:
		if decision.Cause != nil {
			h.handleDeliveryError(ctx, decision.Cause, metadata)
		}
		session.ResetOffset(message.Topic, message.Partition, message.Offset, offsetMetadata(decision))
		h.broker.observe(ctx, eventFromMetadata(EventOffsetReset, metadata, decision.Cause))
	case capmq.DecisionDeadLetter:
		if decision.Cause != nil {
			h.handleDeliveryError(ctx, decision.Cause, metadata)
		}
		session.MarkOffset(message.Topic, message.Partition, message.Offset+1, offsetMetadata(decision))
		session.Commit()
		h.broker.observe(ctx, eventFromMetadata(EventDeadLetter, metadata, decision.Cause))
	default:
		err := fmt.Errorf("unsupported mq decision action %q", decision.Action)
		h.handleDeliveryError(ctx, err, metadata)
		return err
	}
	return nil
}

func (h *consumerGroupHandler) handleDeliveryError(ctx context.Context, err error, metadata capmq.Metadata) {
	if h.subscription.ErrorHandler != nil {
		h.subscription.ErrorHandler.HandleConsumerError(ctx, err, metadata)
		return
	}
	h.broker.handleConsumerError(ctx, err, metadata)
}

func (h *consumerGroupHandler) assignment(session ibmsarama.ConsumerGroupSession) Assignment {
	claims := session.Claims()
	copied := make(map[string][]int32, len(claims))
	for topic, partitions := range claims {
		copied[topic] = append([]int32(nil), partitions...)
	}
	return Assignment{
		Group:        h.subscription.Group,
		MemberID:     session.MemberID(),
		GenerationID: session.GenerationID(),
		Claims:       copied,
	}
}

func toProducerMessage(message capmq.Message) *ibmsarama.ProducerMessage {
	headers := message.Headers
	if len(headers) == 0 {
		headers = message.Header
	}
	producerMessage := &ibmsarama.ProducerMessage{
		Topic:   message.Topic,
		Value:   ibmsarama.ByteEncoder(append([]byte(nil), message.Body...)),
		Headers: toRecordHeaders(headers),
	}
	if message.Key != "" {
		producerMessage.Key = ibmsarama.StringEncoder(message.Key)
	}
	if !message.Metadata.PublishedAt.IsZero() {
		producerMessage.Timestamp = message.Metadata.PublishedAt
	}
	return producerMessage
}

func fromConsumerMessage(message *ibmsarama.ConsumerMessage, subscription capmq.Subscription, session ibmsarama.ConsumerGroupSession) capmq.Message {
	headers := fromRecordHeaders(message.Headers)
	return capmq.Message{
		Topic:   message.Topic,
		Key:     string(message.Key),
		Body:    append([]byte(nil), message.Value...),
		Headers: headers,
		Header:  cloneStringMap(headers),
		Metadata: capmq.Metadata{
			Partition:    int(message.Partition),
			Offset:       message.Offset,
			ReceivedAt:   message.Timestamp,
			Group:        subscription.Group,
			ClientID:     subscription.ClientID,
			SessionID:    subscription.SessionID,
			MemberID:     session.MemberID(),
			GenerationID: session.GenerationID(),
			Attributes: map[string]string{
				"topic":                  message.Topic,
				"high_water_mark_offset": "",
			},
		},
	}
}

func toRecordHeaders(headers map[string]string) []ibmsarama.RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	values := make([]ibmsarama.RecordHeader, 0, len(headers))
	for key, value := range headers {
		values = append(values, ibmsarama.RecordHeader{Key: []byte(key), Value: []byte(value)})
	}
	return values
}

func fromRecordHeaders(headers []*ibmsarama.RecordHeader) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	values := make(map[string]string, len(headers))
	for _, header := range headers {
		values[string(header.Key)] = string(header.Value)
	}
	return values
}

func offsetMetadata(decision capmq.Decision) string {
	if decision.Metadata == nil {
		return ""
	}
	return decision.Metadata["offset_metadata"]
}

func eventFromMetadata(kind EventKind, metadata capmq.Metadata, err error) BrokerEvent {
	return BrokerEvent{
		Kind:      kind,
		Topic:     metadata.Attributes["topic"],
		Group:     metadata.Group,
		Partition: metadata.Partition,
		Offset:    metadata.Offset,
		MemberID:  metadata.MemberID,
		Err:       err,
	}
}

func maxInFlight(value int) int {
	if value <= 0 {
		return 1
	}
	return value
}

func topicList(topic string) []string {
	if topic == "" {
		return nil
	}
	return []string{topic}
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
