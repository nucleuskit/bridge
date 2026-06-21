package nats

import (
	"context"
	"errors"
	"testing"

	caphealth "github.com/nucleuskit/cap/health"
	capmq "github.com/nucleuskit/cap/mq"
)

func TestBrokerPublishesAndSubscribesBySubject(t *testing.T) {
	broker, err := New(Config{Subject: "orders.created", Group: "workers", ClientID: "worker-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = broker.Close() }()

	var producer capmq.Producer = broker
	var consumer capmq.Consumer = broker
	var groupConsumer capmq.GroupConsumer = broker

	if err := producer.Publish(context.Background(), capmq.Message{
		ID:      "msg-1",
		Key:     "order-1",
		Body:    []byte("created"),
		Headers: map[string]string{"trace_id": "trace-1"},
	}); err != nil {
		t.Fatal(err)
	}

	delivery := readNATSDelivery(t, consumer)
	if delivery.Message.Topic != "orders.created" {
		t.Fatalf("expected default subject as topic, got %q", delivery.Message.Topic)
	}
	if string(delivery.Message.Body) != "created" || delivery.Message.Headers["trace_id"] != "trace-1" {
		t.Fatalf("unexpected delivery message: %#v", delivery.Message)
	}
	if delivery.Message.Metadata.Group != "workers" || delivery.Message.Metadata.ClientID != "worker-a" {
		t.Fatalf("unexpected metadata: %#v", delivery.Message.Metadata)
	}
	if err := delivery.AckMessage(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := producer.Publish(context.Background(), capmq.Message{Topic: "orders.updated", ID: "msg-2"}); err != nil {
		t.Fatal(err)
	}
	otherGroupDelivery := readNATSGroupDelivery(t, groupConsumer, capmq.Subscription{
		Group:  "audit",
		Topics: []string{"orders.updated"},
	})
	if otherGroupDelivery.Message.Topic != "orders.updated" || otherGroupDelivery.Message.Metadata.Group != "audit" {
		t.Fatalf("unexpected group delivery: %#v", otherGroupDelivery.Message)
	}
}

func TestBrokerBatchPublishesSubjects(t *testing.T) {
	broker, err := New(Config{Subject: "events"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = broker.Close() }()

	var batchProducer capmq.BatchProducer = broker
	results, err := batchProducer.PublishBatch(context.Background(),
		capmq.Message{ID: "msg-1", Body: []byte("one")},
		capmq.Message{ID: "msg-2", Topic: "events.audit", Body: []byte("two")},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected two publish results, got %d", len(results))
	}
	if results[0].Topic != "events" || results[0].Offset != 0 || results[0].Timestamp.IsZero() {
		t.Fatalf("unexpected first result: %#v", results[0])
	}
	if results[1].Topic != "events.audit" || results[1].Offset != 0 || results[1].Timestamp.IsZero() {
		t.Fatalf("unexpected second result: %#v", results[1])
	}

	first := readNATSGroupDelivery(t, broker, capmq.Subscription{Group: "workers", Topics: []string{"events"}})
	second := readNATSGroupDelivery(t, broker, capmq.Subscription{Group: "workers", Topics: []string{"events.audit"}})
	if first.Message.ID != "msg-1" || second.Message.ID != "msg-2" {
		t.Fatalf("unexpected batch deliveries: %#v %#v", first.Message, second.Message)
	}
}

func TestBrokerAckNackAndDecisionSemantics(t *testing.T) {
	var handled []error
	broker, err := New(Config{
		Subject: "jobs",
		ErrorHandler: capmq.ErrorHandlerFunc(func(ctx context.Context, err error, metadata capmq.Metadata) {
			handled = append(handled, err)
		}),
		DeadLetter: capmq.DeadLetterPolicy{Topic: "jobs.dlq", Reason: "failed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = broker.Close() }()

	if err := broker.Publish(context.Background(), capmq.Message{ID: "job-1"}); err != nil {
		t.Fatal(err)
	}
	first := readNATSGroupDelivery(t, broker, capmq.Subscription{Group: "workers", Topics: []string{"jobs"}})
	nackErr := errors.New("handler failed")
	if err := first.NackMessage(context.Background(), nackErr); err != nil {
		t.Fatal(err)
	}
	if len(handled) != 1 || !errors.Is(handled[0], nackErr) {
		t.Fatalf("expected nack error handler, got %#v", handled)
	}

	redelivered := readNATSGroupDelivery(t, broker, capmq.Subscription{Group: "workers", Topics: []string{"jobs"}})
	if redelivered.Message.Metadata.DeliveryAttempt != 2 {
		t.Fatalf("expected second delivery attempt, got %#v", redelivered.Message.Metadata)
	}
	if err := redelivered.DeadLetterMessage(context.Background(), nackErr, capmq.DeadLetterMetadata{}); err != nil {
		t.Fatal(err)
	}
	if deadLetters := broker.DeadLetters("jobs.dlq"); len(deadLetters) != 1 {
		t.Fatalf("expected one dead letter, got %d", len(deadLetters))
	} else if deadLetters[0].Metadata.DeadLetter == nil || deadLetters[0].Metadata.DeadLetter.OriginalTopic != "jobs" {
		t.Fatalf("unexpected dead letter: %#v", deadLetters[0])
	}

	if err := broker.Publish(context.Background(), capmq.Message{ID: "job-2"}); err != nil {
		t.Fatal(err)
	}
	acked := readNATSGroupDelivery(t, broker, capmq.Subscription{Group: "workers", Topics: []string{"jobs"}})
	if acked.Message.ID != "job-2" {
		t.Fatalf("expected second job after dead letter commit, got %#v", acked.Message)
	}
	if err := acked.Decide(context.Background(), capmq.Decision{Action: capmq.DecisionAck}); err != nil {
		t.Fatal(err)
	}
	deliveries, err := broker.Subscribe(context.Background(), capmq.Subscription{Group: "workers", Topics: []string{"jobs"}})
	if err != nil {
		t.Fatal(err)
	}
	if delivery, ok := <-deliveries; ok {
		t.Fatalf("expected acked group to have no pending jobs, got %#v", delivery)
	}
}

func TestBrokerReportsHealthAndClose(t *testing.T) {
	var _ caphealth.Reporter = (*Broker)(nil)

	broker, err := New(Config{Servers: []string{"nats://placeholder:4222"}, Subject: "events", Group: "workers"})
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
	if report.Metadata["provider"] != "nats" || report.Metadata["subject"] != "events" || report.Metadata["group"] != "workers" {
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
		t.Fatalf("expected closed broker down report, got %#v", report)
	}
	err = broker.Publish(context.Background(), capmq.Message{ID: "after-close"})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed after close, got %v", err)
	}
}

func readNATSDelivery(t *testing.T, consumer capmq.Consumer) capmq.Delivery {
	t.Helper()
	deliveries, err := consumer.Consume(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	delivery, ok := <-deliveries
	if !ok {
		t.Fatal("expected one delivery")
	}
	return delivery
}

func readNATSGroupDelivery(t *testing.T, consumer capmq.GroupConsumer, subscription capmq.Subscription) capmq.Delivery {
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
