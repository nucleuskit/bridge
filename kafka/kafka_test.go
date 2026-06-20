package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	caphealth "github.com/nucleuskit/nucleus/cap/health"
	capmq "github.com/nucleuskit/nucleus/cap/mq"
)

func TestBrokerImplementsMQCapabilityInMemory(t *testing.T) {
	broker, err := New(Config{Brokers: []string{"placeholder:9092"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = broker.Close() }()

	var producer capmq.Producer = broker
	var batchProducer capmq.BatchProducer = broker
	var consumer capmq.Consumer = broker
	var groupConsumer capmq.GroupConsumer = broker
	if err := producer.Publish(context.Background(), capmq.Message{Topic: "events", Body: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if _, err := batchProducer.PublishBatch(context.Background(), capmq.Message{Topic: "events", Body: []byte("batch")}); err != nil {
		t.Fatal(err)
	}
	deliveries, err := consumer.Consume(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	delivery, ok := <-deliveries
	if !ok {
		t.Fatal("expected one delivery")
	}
	if string(delivery.Message.Body) != "hello" {
		t.Fatalf("expected hello, got %q", delivery.Message.Body)
	}
	if err := delivery.AckMessage(context.Background()); err != nil {
		t.Fatal(err)
	}
	groupDeliveries, err := groupConsumer.Subscribe(context.Background(), capmq.Subscription{Group: "another", Topics: []string{"events"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := <-groupDeliveries; !ok {
		t.Fatal("expected another consumer group to receive the topic backlog")
	}
}

func TestBrokerReportsMQHealth(t *testing.T) {
	var _ caphealth.Reporter = (*Broker)(nil)

	broker, err := New(Config{Brokers: []string{"placeholder:9092"}, Topic: "events", Group: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	report, err := broker.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Capability != "mq" || report.Status != caphealth.StatusReady {
		t.Fatalf("expected ready mq report, got %#v", report)
	}
	if report.Metadata["provider"] != "kafka" || report.Metadata["topic"] != "events" || report.Metadata["group"] != "workers" {
		t.Fatalf("unexpected health metadata: %#v", report.Metadata)
	}

	if err := broker.Close(); err != nil {
		t.Fatal(err)
	}
	report, err = broker.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDown || report.Message == "" {
		t.Fatalf("expected closed broker to report down, got %#v", report)
	}
}

func TestBrokerReportsDegradedWhenMQConfigIsMissing(t *testing.T) {
	broker, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	report, err := broker.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDegraded || report.Message == "" {
		t.Fatalf("expected missing mq config to report degraded, got %#v", report)
	}
}

func TestBrokerProducerCallbackReceivesSuccessAndError(t *testing.T) {
	var successes []capmq.PublishResult
	var errorsSeen []error
	callback := capmq.ProducerCallbackFunc{
		Success: func(ctx context.Context, message capmq.Message, result capmq.PublishResult) {
			successes = append(successes, result)
		},
		Error: func(ctx context.Context, message capmq.Message, err error) {
			errorsSeen = append(errorsSeen, err)
		},
	}
	broker, err := New(Config{Topic: "events", Callback: callback})
	if err != nil {
		t.Fatal(err)
	}

	if err := broker.Publish(context.Background(), capmq.Message{ID: "msg-1", Body: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if len(successes) != 1 {
		t.Fatalf("expected one success callback, got %d", len(successes))
	}
	if successes[0].Topic != "events" || successes[0].Offset != 0 || successes[0].Timestamp.IsZero() {
		t.Fatalf("unexpected publish result: %#v", successes[0])
	}

	brokerWithoutTopic, err := New(Config{Callback: callback})
	if err != nil {
		t.Fatal(err)
	}
	err = brokerWithoutTopic.Publish(context.Background(), capmq.Message{ID: "missing-topic"})
	if !errors.Is(err, ErrMissingTopic) {
		t.Fatalf("expected ErrMissingTopic, got %v", err)
	}
	if len(errorsSeen) != 1 || !errors.Is(errorsSeen[0], ErrMissingTopic) {
		t.Fatalf("expected callback error, got %#v", errorsSeen)
	}
}

func TestBrokerConsumerGroupAckControlsOffset(t *testing.T) {
	broker, err := New(Config{Topic: "jobs"})
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), capmq.Message{ID: "job-1", Body: []byte("run")}); err != nil {
		t.Fatal(err)
	}

	first := readOneDelivery(t, broker, capmq.Subscription{Group: "workers", Topics: []string{"jobs"}})
	if first.Message.Metadata.Group != "workers" || first.Message.Metadata.DeliveryAttempt != 1 {
		t.Fatalf("unexpected first delivery metadata: %#v", first.Message.Metadata)
	}
	second := readOneDelivery(t, broker, capmq.Subscription{Group: "workers", Topics: []string{"jobs"}})
	if second.Message.Metadata.DeliveryAttempt != 2 {
		t.Fatalf("expected redelivery attempt 2 before ack, got %#v", second.Message.Metadata)
	}
	if err := second.AckMessage(context.Background()); err != nil {
		t.Fatal(err)
	}
	deliveries, err := broker.Subscribe(context.Background(), capmq.Subscription{Group: "workers", Topics: []string{"jobs"}})
	if err != nil {
		t.Fatal(err)
	}
	if delivery, ok := <-deliveries; ok {
		t.Fatalf("expected committed group offset to hide delivery, got %#v", delivery)
	}

	otherGroup := readOneDelivery(t, broker, capmq.Subscription{Group: "audit", Topics: []string{"jobs"}})
	if otherGroup.Message.Metadata.Group != "audit" || otherGroup.Message.Metadata.DeliveryAttempt != 1 {
		t.Fatalf("expected independent group attempt, got %#v", otherGroup.Message.Metadata)
	}
}

func TestBrokerDeadLettersAndCommitsDelivery(t *testing.T) {
	broker, err := New(Config{Topic: "jobs", DeadLetter: capmq.DeadLetterPolicy{Topic: "jobs.dlq", Reason: "handler_failed"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), capmq.Message{ID: "job-1", Body: []byte("run")}); err != nil {
		t.Fatal(err)
	}
	delivery := readOneDelivery(t, broker, capmq.Subscription{Group: "workers", Topics: []string{"jobs"}})
	cause := errors.New("boom")
	if err := delivery.DeadLetterMessage(context.Background(), cause, capmq.DeadLetterMetadata{FailedAt: time.Unix(10, 0)}); err != nil {
		t.Fatal(err)
	}

	deadLetters := broker.DeadLetters("jobs.dlq")
	if len(deadLetters) != 1 {
		t.Fatalf("expected one dead letter, got %d", len(deadLetters))
	}
	dead := deadLetters[0]
	if dead.Topic != "jobs.dlq" || dead.Metadata.DeadLetter == nil {
		t.Fatalf("unexpected dead letter message: %#v", dead)
	}
	if dead.Metadata.DeadLetter.OriginalTopic != "jobs" || dead.Metadata.DeadLetter.OriginalGroup != "workers" {
		t.Fatalf("unexpected dead letter metadata: %#v", dead.Metadata.DeadLetter)
	}
	deliveries, err := broker.Subscribe(context.Background(), capmq.Subscription{Group: "workers", Topics: []string{"jobs"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := <-deliveries; ok {
		t.Fatal("expected dead-lettered delivery to be committed")
	}
}

func TestBrokerStartOffsetLatestSkipsBacklog(t *testing.T) {
	broker, err := New(Config{Topic: "events"})
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), capmq.Message{ID: "old"}); err != nil {
		t.Fatal(err)
	}
	deliveries, err := broker.Subscribe(context.Background(), capmq.Subscription{
		Group:       "late",
		Topics:      []string{"events"},
		StartOffset: capmq.OffsetResetLatest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := <-deliveries; ok {
		t.Fatal("expected latest subscription to skip existing backlog")
	}
	if err := broker.Publish(context.Background(), capmq.Message{ID: "new"}); err != nil {
		t.Fatal(err)
	}
	delivery := readOneDelivery(t, broker, capmq.Subscription{Group: "late", Topics: []string{"events"}})
	if delivery.Message.ID != "new" {
		t.Fatalf("expected new message, got %#v", delivery.Message)
	}
}

func readOneDelivery(t *testing.T, consumer capmq.GroupConsumer, subscription capmq.Subscription) capmq.Delivery {
	t.Helper()
	deliveries, err := consumer.Subscribe(context.Background(), subscription)
	if err != nil {
		t.Fatal(err)
	}
	delivery, ok := <-deliveries
	if !ok {
		t.Fatal("expected one delivery")
	}
	return delivery
}
