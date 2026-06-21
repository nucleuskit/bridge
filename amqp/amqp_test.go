package amqp

import (
	"context"
	"errors"
	"testing"
	"time"

	caphealth "github.com/nucleuskit/cap/health"
	capmq "github.com/nucleuskit/cap/mq"
)

func TestBrokerPublishesAndSubscribesByExchangeRoutingKeyAndQueue(t *testing.T) {
	broker, err := New(Config{
		Exchange:   "orders",
		RoutingKey: "created",
		Queue:      "order-created",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = broker.Close() }()

	var producer capmq.Producer = broker
	var consumer capmq.Consumer = broker
	var groupConsumer capmq.GroupConsumer = broker

	if err := producer.Publish(context.Background(), capmq.Message{
		ID:     "msg-1",
		Body:   []byte("order created"),
		Header: map[string]string{"content-type": "application/json"},
	}); err != nil {
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
	if delivery.Message.Topic != "orders" || delivery.Message.Key != "created" {
		t.Fatalf("expected AMQP exchange/routing key on message, got %#v", delivery.Message)
	}
	if delivery.Message.Metadata.Attributes["amqp.queue"] != "order-created" {
		t.Fatalf("expected queue metadata, got %#v", delivery.Message.Metadata.Attributes)
	}
	if string(delivery.Message.Body) != "order created" {
		t.Fatalf("expected body to round trip, got %q", delivery.Message.Body)
	}
	if err := delivery.AckMessage(context.Background()); err != nil {
		t.Fatal(err)
	}

	otherQueue, err := groupConsumer.Subscribe(context.Background(), capmq.Subscription{
		Group:  "billing",
		Topics: []string{"order-created"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := <-otherQueue; !ok {
		t.Fatal("expected named queue subscription to receive backlog")
	}
}

func TestBrokerPublishBatchRoutesMessages(t *testing.T) {
	broker, err := New(Config{Exchange: "events", Queue: "events-q"})
	if err != nil {
		t.Fatal(err)
	}

	var batchProducer capmq.BatchProducer = broker
	results, err := batchProducer.PublishBatch(context.Background(),
		capmq.Message{ID: "a", Key: "alpha", Body: []byte("one")},
		capmq.Message{ID: "b", Key: "beta", Body: []byte("two")},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected two publish results, got %d", len(results))
	}
	if results[0].Topic != "events" || results[0].Offset != 0 || results[1].Offset != 1 {
		t.Fatalf("unexpected publish results: %#v", results)
	}

	deliveries, err := broker.Subscribe(context.Background(), capmq.Subscription{Group: "workers", Topics: []string{"events-q"}})
	if err != nil {
		t.Fatal(err)
	}
	first, ok := <-deliveries
	if !ok {
		t.Fatal("expected first delivery")
	}
	second, ok := <-deliveries
	if !ok {
		t.Fatal("expected second delivery")
	}
	if first.Message.ID != "a" || second.Message.ID != "b" {
		t.Fatalf("expected batch order to be preserved, got %q then %q", first.Message.ID, second.Message.ID)
	}
}

func TestBrokerAckNackAndDecisionControlQueueOffset(t *testing.T) {
	var handled []error
	broker, err := New(Config{
		Exchange: "jobs",
		Queue:    "jobs-q",
		ErrorHandler: capmq.ErrorHandlerFunc(func(ctx context.Context, err error, metadata capmq.Metadata) {
			handled = append(handled, err)
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), capmq.Message{ID: "job-1", Body: []byte("run")}); err != nil {
		t.Fatal(err)
	}

	first := readOneDelivery(t, broker, capmq.Subscription{Group: "workers", Topics: []string{"jobs-q"}})
	if err := first.NackMessage(context.Background(), errors.New("temporary")); err != nil {
		t.Fatal(err)
	}
	if len(handled) != 1 {
		t.Fatalf("expected nack to call error handler, got %d", len(handled))
	}

	second := readOneDelivery(t, broker, capmq.Subscription{Group: "workers", Topics: []string{"jobs-q"}})
	if second.Message.Metadata.DeliveryAttempt != 2 {
		t.Fatalf("expected redelivery attempt 2, got %#v", second.Message.Metadata)
	}
	if err := second.Decide(context.Background(), capmq.Decision{Action: capmq.DecisionAck}); err != nil {
		t.Fatal(err)
	}

	deliveries, err := broker.Subscribe(context.Background(), capmq.Subscription{Group: "workers", Topics: []string{"jobs-q"}})
	if err != nil {
		t.Fatal(err)
	}
	if delivery, ok := <-deliveries; ok {
		t.Fatalf("expected acked delivery to be committed, got %#v", delivery)
	}
}

func TestBrokerDeadLetterDecisionCommitsDelivery(t *testing.T) {
	broker, err := New(Config{
		Exchange:   "jobs",
		RoutingKey: "run",
		Queue:      "jobs-q",
		DeadLetter: capmq.DeadLetterPolicy{Topic: "jobs.dlq", Reason: "failed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), capmq.Message{ID: "job-1"}); err != nil {
		t.Fatal(err)
	}

	delivery := readOneDelivery(t, broker, capmq.Subscription{Group: "workers", Topics: []string{"jobs-q"}})
	if err := delivery.DeadLetterMessage(context.Background(), errors.New("boom"), capmq.DeadLetterMetadata{FailedAt: time.Unix(10, 0)}); err != nil {
		t.Fatal(err)
	}

	deadLetters := broker.DeadLetters("jobs.dlq")
	if len(deadLetters) != 1 {
		t.Fatalf("expected one dead letter, got %d", len(deadLetters))
	}
	if deadLetters[0].Metadata.DeadLetter == nil || deadLetters[0].Metadata.DeadLetter.OriginalTopic != "jobs" {
		t.Fatalf("unexpected dead letter metadata: %#v", deadLetters[0])
	}
	deliveries, err := broker.Subscribe(context.Background(), capmq.Subscription{Group: "workers", Topics: []string{"jobs-q"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := <-deliveries; ok {
		t.Fatal("expected dead-lettered delivery to be committed")
	}
}

func TestBrokerReportsHealthAndClose(t *testing.T) {
	var _ caphealth.Reporter = (*Broker)(nil)

	broker, err := New(Config{Exchange: "events", RoutingKey: "created", Queue: "events-q"})
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
	if report.Metadata["provider"] != "amqp" || report.Metadata["exchange"] != "events" || report.Metadata["queue"] != "events-q" {
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

func TestBrokerReportsDegradedWhenNoAMQPRouteIsConfigured(t *testing.T) {
	broker, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	report, err := broker.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDegraded || report.Message == "" {
		t.Fatalf("expected degraded report, got %#v", report)
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
