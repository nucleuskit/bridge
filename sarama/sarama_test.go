package sarama

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	ibmsarama "github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	caphealth "github.com/nucleuskit/cap/health"
	capmq "github.com/nucleuskit/cap/mq"
)

func TestBrokerPublishesThroughSaramaProducerWithCallbacksAndObservability(t *testing.T) {
	config := ibmsarama.NewConfig()
	config.Producer.Return.Successes = true
	producer := mocks.NewSyncProducer(t, config)
	producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(message *ibmsarama.ProducerMessage) error {
		if message.Topic != "orders" {
			return fmt.Errorf("topic = %q", message.Topic)
		}
		key, _ := message.Key.Encode()
		if string(key) != "order-1" {
			return fmt.Errorf("key = %q", key)
		}
		value, _ := message.Value.Encode()
		if string(value) != "created" {
			return fmt.Errorf("value = %q", value)
		}
		if got := headerValue(message.Headers, "trace_id"); got != "trace-1" {
			return fmt.Errorf("trace_id header = %q", got)
		}
		return nil
	})

	var callbackResult capmq.PublishResult
	callback := capmq.ProducerCallbackFunc{
		Success: func(ctx context.Context, message capmq.Message, result capmq.PublishResult) {
			callbackResult = result
		},
	}
	observer := &recordingObserver{}
	broker, err := New(Config{
		Brokers:  []string{"unused:9092"},
		Topic:    "orders",
		Group:    "workers",
		Producer: producer,
		Callback: callback,
		Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = broker.Close() }()

	if err := broker.Publish(context.Background(), capmq.Message{
		ID:      "msg-1",
		Key:     "order-1",
		Body:    []byte("created"),
		Headers: map[string]string{"trace_id": "trace-1"},
	}); err != nil {
		t.Fatal(err)
	}

	if callbackResult.Topic != "orders" || callbackResult.Partition < 0 || callbackResult.Offset <= 0 {
		t.Fatalf("unexpected callback result: %#v", callbackResult)
	}
	if !observer.hasKind(EventPublishSuccess) {
		t.Fatalf("expected publish success event, got %#v", observer.events)
	}
}

func TestBrokerReportsPublishErrorThroughCallbackAndObserver(t *testing.T) {
	produceErr := errors.New("broker rejected write")
	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageAndFail(produceErr)
	var callbackErr error
	observer := &recordingObserver{}
	broker, err := New(Config{
		Topic:    "orders",
		Producer: producer,
		Callback: capmq.ProducerCallbackFunc{
			Error: func(ctx context.Context, message capmq.Message, err error) {
				callbackErr = err
			},
		},
		Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = broker.Close() }()

	err = broker.Publish(context.Background(), capmq.Message{ID: "msg-1"})
	if !errors.Is(err, produceErr) {
		t.Fatalf("expected produce error, got %v", err)
	}
	if !errors.Is(callbackErr, produceErr) {
		t.Fatalf("expected callback error, got %v", callbackErr)
	}
	if !observer.hasKind(EventPublishError) {
		t.Fatalf("expected publish error event, got %#v", observer.events)
	}
}

func TestBrokerConsumesConsumerGroupWithRebalanceHooksAndAckDecision(t *testing.T) {
	group := newFakeConsumerGroup()
	session := newFakeSession("member-1", 7)
	claim := newFakeClaim("orders", 2, 10, 12)
	claim.push(&ibmsarama.ConsumerMessage{
		Topic:     "orders",
		Partition: 2,
		Offset:    10,
		Key:       []byte("order-1"),
		Value:     []byte("created"),
		Timestamp: time.Unix(20, 0),
		Headers: []*ibmsarama.RecordHeader{
			{Key: []byte("trace_id"), Value: []byte("trace-2")},
		},
	})
	claim.close()
	group.nextSession = session
	group.nextClaim = claim
	observer := &recordingObserver{}
	rebalance := &recordingRebalanceHandler{}
	broker, err := New(Config{
		Group:            "workers",
		ConsumerGroup:    group,
		RebalanceHandler: rebalance,
		Observer:         observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = broker.Close() }()

	deliveries, err := broker.Subscribe(context.Background(), capmq.Subscription{Topics: []string{"orders"}})
	if err != nil {
		t.Fatal(err)
	}
	delivery, ok := <-deliveries
	if !ok {
		t.Fatal("expected one delivery")
	}
	if delivery.Message.Topic != "orders" || delivery.Message.Metadata.Partition != 2 || delivery.Message.Metadata.Offset != 10 {
		t.Fatalf("unexpected delivery metadata: %#v", delivery.Message)
	}
	if delivery.Message.Metadata.Group != "workers" || delivery.Message.Metadata.MemberID != "member-1" || delivery.Message.Metadata.GenerationID != 7 {
		t.Fatalf("unexpected group metadata: %#v", delivery.Message.Metadata)
	}
	if got := delivery.Message.Headers["trace_id"]; got != "trace-2" {
		t.Fatalf("trace header = %q", got)
	}

	if err := delivery.AckMessage(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := session.marked; !reflect.DeepEqual(got, []markedOffset{{topic: "orders", partition: 2, offset: 11}}) {
		t.Fatalf("unexpected marked offsets: %#v", got)
	}
	if !rebalance.setupSeen || !rebalance.cleanupSeen {
		t.Fatalf("expected rebalance hooks, got %#v", rebalance)
	}
	if !observer.hasKind(EventRebalanceStart) || !observer.hasKind(EventDelivery) || !observer.hasKind(EventOffsetCommit) || !observer.hasKind(EventRebalanceEnd) {
		t.Fatalf("missing observer event, got %#v", observer.events)
	}
	if _, ok := <-deliveries; ok {
		t.Fatal("expected delivery channel to close after fake claim drains")
	}
}

func TestBrokerOffsetDecisionsHandleNackRetryAndDeadLetterWithoutSaramaLeakage(t *testing.T) {
	group := newFakeConsumerGroup()
	session := newFakeSession("member-1", 1)
	claim := newFakeClaim("orders", 0, 4, 8)
	claim.push(&ibmsarama.ConsumerMessage{Topic: "orders", Partition: 0, Offset: 4, Value: []byte("bad")})
	claim.close()
	group.nextSession = session
	group.nextClaim = claim
	var handled []error
	observer := &recordingObserver{}
	broker, err := New(Config{
		Group:         "workers",
		ConsumerGroup: group,
		ErrorHandler: capmq.ErrorHandlerFunc(func(ctx context.Context, err error, metadata capmq.Metadata) {
			handled = append(handled, err)
		}),
		Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = broker.Close() }()

	deliveries, err := broker.Subscribe(context.Background(), capmq.Subscription{Topics: []string{"orders"}})
	if err != nil {
		t.Fatal(err)
	}
	delivery := <-deliveries
	nackErr := errors.New("handler failed")
	if err := delivery.NackMessage(context.Background(), nackErr); err != nil {
		t.Fatal(err)
	}
	if len(handled) != 1 || !errors.Is(handled[0], nackErr) {
		t.Fatalf("expected nack error handler, got %#v", handled)
	}
	if err := delivery.RetryMessage(context.Background(), nackErr, time.Second); err != nil {
		t.Fatal(err)
	}
	if got := session.reset; !reflect.DeepEqual(got, []markedOffset{{topic: "orders", partition: 0, offset: 4}}) {
		t.Fatalf("unexpected reset offsets: %#v", got)
	}
	if err := delivery.DeadLetterMessage(context.Background(), nackErr, capmq.DeadLetterMetadata{Topic: "orders.dlq"}); err != nil {
		t.Fatal(err)
	}
	if got := session.marked; !reflect.DeepEqual(got, []markedOffset{{topic: "orders", partition: 0, offset: 5}}) {
		t.Fatalf("unexpected dead letter commit offsets: %#v", got)
	}
	if !observer.hasKind(EventOffsetReset) || !observer.hasKind(EventDeadLetter) {
		t.Fatalf("missing observer decisions, got %#v", observer.events)
	}
}

func TestBrokerForwardsConsumerGroupErrorsToErrorHandlerAndHealth(t *testing.T) {
	group := newFakeConsumerGroup()
	group.errors <- errors.New("rebalance failed")
	close(group.errors)
	var handled []error
	broker, err := New(Config{
		Brokers:       []string{"broker-a:9092", "broker-b:9092"},
		Topic:         "orders",
		Group:         "workers",
		ConsumerGroup: group,
		ErrorHandler: capmq.ErrorHandlerFunc(func(ctx context.Context, err error, metadata capmq.Metadata) {
			handled = append(handled, err)
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = broker.Close() }()

	broker.PollErrors(context.Background())
	if len(handled) != 1 || handled[0].Error() != "rebalance failed" {
		t.Fatalf("expected consumer group error handler, got %#v", handled)
	}

	var _ caphealth.Reporter = broker
	report, err := broker.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Capability != "mq" || report.Status != caphealth.StatusReady || report.Metadata["provider"] != "sarama" {
		t.Fatalf("unexpected report: %#v", report)
	}
	if report.Metadata["brokers"] != "2" || report.Metadata["topic"] != "orders" || report.Metadata["group"] != "workers" {
		t.Fatalf("unexpected health metadata: %#v", report.Metadata)
	}
}

func headerValue(headers []ibmsarama.RecordHeader, key string) string {
	for _, header := range headers {
		if string(header.Key) == key {
			return string(header.Value)
		}
	}
	return ""
}

type recordingObserver struct {
	mu     sync.Mutex
	events []BrokerEvent
}

func (o *recordingObserver) ObserveBrokerEvent(ctx context.Context, event BrokerEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, event)
}

func (o *recordingObserver) hasKind(kind EventKind) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, event := range o.events {
		if event.Kind == kind {
			return true
		}
	}
	return false
}

type recordingRebalanceHandler struct {
	setupSeen   bool
	cleanupSeen bool
}

func (h *recordingRebalanceHandler) OnRebalanceStart(ctx context.Context, assignment Assignment) {
	h.setupSeen = true
}

func (h *recordingRebalanceHandler) OnRebalanceEnd(ctx context.Context, assignment Assignment) {
	h.cleanupSeen = true
}

type fakeConsumerGroup struct {
	nextSession *fakeSession
	nextClaim   *fakeClaim
	errors      chan error
	closed      bool
}

func newFakeConsumerGroup() *fakeConsumerGroup {
	return &fakeConsumerGroup{errors: make(chan error, 8)}
}

func (g *fakeConsumerGroup) Consume(ctx context.Context, topics []string, handler ibmsarama.ConsumerGroupHandler) error {
	if err := handler.Setup(g.nextSession); err != nil {
		return err
	}
	if err := handler.ConsumeClaim(g.nextSession, g.nextClaim); err != nil {
		return err
	}
	return handler.Cleanup(g.nextSession)
}

func (g *fakeConsumerGroup) Errors() <-chan error { return g.errors }
func (g *fakeConsumerGroup) Close() error {
	g.closed = true
	return nil
}
func (g *fakeConsumerGroup) Pause(map[string][]int32)  {}
func (g *fakeConsumerGroup) Resume(map[string][]int32) {}
func (g *fakeConsumerGroup) PauseAll()                 {}
func (g *fakeConsumerGroup) ResumeAll()                {}

type markedOffset struct {
	topic     string
	partition int32
	offset    int64
	metadata  string
}

type fakeSession struct {
	ctx          context.Context
	memberID     string
	generationID int32
	claims       map[string][]int32
	marked       []markedOffset
	reset        []markedOffset
	commits      int
}

func newFakeSession(memberID string, generationID int32) *fakeSession {
	return &fakeSession{
		ctx:          context.Background(),
		memberID:     memberID,
		generationID: generationID,
		claims:       map[string][]int32{"orders": {0, 2}},
	}
}

func (s *fakeSession) Claims() map[string][]int32 { return s.claims }
func (s *fakeSession) MemberID() string           { return s.memberID }
func (s *fakeSession) GenerationID() int32        { return s.generationID }
func (s *fakeSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	s.marked = append(s.marked, markedOffset{topic: topic, partition: partition, offset: offset, metadata: metadata})
}
func (s *fakeSession) Commit() { s.commits++ }
func (s *fakeSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	s.reset = append(s.reset, markedOffset{topic: topic, partition: partition, offset: offset, metadata: metadata})
}
func (s *fakeSession) MarkMessage(message *ibmsarama.ConsumerMessage, metadata string) {
	s.MarkOffset(message.Topic, message.Partition, message.Offset+1, metadata)
}
func (s *fakeSession) Context() context.Context { return s.ctx }

type fakeClaim struct {
	topic     string
	partition int32
	initial   int64
	high      int64
	messages  chan *ibmsarama.ConsumerMessage
}

func newFakeClaim(topic string, partition int32, initial int64, high int64) *fakeClaim {
	return &fakeClaim{
		topic:     topic,
		partition: partition,
		initial:   initial,
		high:      high,
		messages:  make(chan *ibmsarama.ConsumerMessage, 8),
	}
}

func (c *fakeClaim) Topic() string                               { return c.topic }
func (c *fakeClaim) Partition() int32                            { return c.partition }
func (c *fakeClaim) InitialOffset() int64                        { return c.initial }
func (c *fakeClaim) HighWaterMarkOffset() int64                  { return c.high }
func (c *fakeClaim) Messages() <-chan *ibmsarama.ConsumerMessage { return c.messages }
func (c *fakeClaim) push(message *ibmsarama.ConsumerMessage)     { c.messages <- message }
func (c *fakeClaim) close()                                      { close(c.messages) }
