package prometheus

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	capmetric "github.com/nucleuskit/cap/metric"
	capmq "github.com/nucleuskit/cap/mq"
	capredis "github.com/nucleuskit/cap/redis"
	capsql "github.com/nucleuskit/cap/sql"
)

func TestMeterImplementsMetricCapabilityInMemory(t *testing.T) {
	meter, err := New(Config{Namespace: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = meter.Close() }()

	var capMeter capmetric.Meter = meter
	capMeter.Counter("requests_total").Add(context.Background(), 2, capmetric.String("route", "/readyz"))
	capMeter.Gauge("queue_depth").Set(context.Background(), 7)

	snapshot := capMeter.Snapshot()
	if snapshot["hello_requests_total"] != 2 {
		t.Fatalf("expected counter value 2, got %#v", snapshot)
	}
	if snapshot["hello_queue_depth"] != 7 {
		t.Fatalf("expected gauge value 7, got %#v", snapshot)
	}
}

func TestMeterSnapshotsLabelsAndHistogramBuckets(t *testing.T) {
	meter, err := New(Config{Namespace: "http"})
	if err != nil {
		t.Fatal(err)
	}

	requests := meter.Counter("server_requests_total", capmetric.WithLabels("method", "route"))
	requests.Add(context.Background(), 2,
		capmetric.String("route", "/readyz"),
		capmetric.String("method", "GET"),
		capmetric.String("ignored", "drop"),
	)

	duration := meter.Histogram("server_duration_ms", capmetric.WithLabels("route"), capmetric.WithBuckets(50, 100))
	duration.Observe(context.Background(), 75, capmetric.String("route", "/readyz"))
	duration.Observe(context.Background(), 125, capmetric.String("route", "/readyz"))

	snapshot := meter.Snapshot()
	counterKey := `http_server_requests_total{method="GET",route="/readyz"}`
	if snapshot[counterKey] != 2 {
		t.Fatalf("counter snapshot = %#v", snapshot)
	}
	if snapshot[`http_server_duration_ms_sum{route="/readyz"}`] != 200 {
		t.Fatalf("histogram sum snapshot = %#v", snapshot)
	}
	if snapshot[`http_server_duration_ms_count{route="/readyz"}`] != 2 {
		t.Fatalf("histogram count snapshot = %#v", snapshot)
	}

	series := meter.SnapshotSeries()
	histogram := findSeries(t, series, capmetric.KindHistogram, "http_server_duration_ms")
	if histogram.Count != 2 || histogram.Sum != 200 {
		t.Fatalf("histogram series = %#v", histogram)
	}
	if len(histogram.Buckets) != 3 {
		t.Fatalf("histogram buckets = %#v", histogram.Buckets)
	}
	if histogram.Buckets[0].UpperBound != 50 || histogram.Buckets[0].Count != 0 {
		t.Fatalf("first bucket = %#v", histogram.Buckets[0])
	}
	if histogram.Buckets[1].UpperBound != 100 || histogram.Buckets[1].Count != 1 {
		t.Fatalf("second bucket = %#v", histogram.Buckets[1])
	}
	if !math.IsInf(histogram.Buckets[2].UpperBound, 1) || histogram.Buckets[2].Count != 2 {
		t.Fatalf("+Inf bucket = %#v", histogram.Buckets[2])
	}
}

func TestMeterWritesPrometheusTextExposition(t *testing.T) {
	meter, err := New(Config{Namespace: "svc"})
	if err != nil {
		t.Fatal(err)
	}

	requests := meter.Counter("requests_total",
		capmetric.WithDescription("HTTP requests by route"),
		capmetric.WithLabels("method", "route"),
	)
	requests.Add(context.Background(), 3,
		capmetric.String("method", "GET"),
		capmetric.String("route", "/readyz"),
	)

	queueDepth := meter.Gauge("queue_depth", capmetric.WithLabels("queue"))
	queueDepth.Set(context.Background(), 5, capmetric.String("queue", "jobs\"fast\\lane\nprimary"))

	duration := meter.Histogram("request_duration_ms",
		capmetric.WithDescription("Request duration"),
		capmetric.WithLabels("route"),
		capmetric.WithBuckets(50, 100),
	)
	duration.Observe(context.Background(), 75, capmetric.String("route", "/readyz"))
	duration.Observe(context.Background(), 125, capmetric.String("route", "/readyz"))

	var output strings.Builder
	written, err := meter.WriteTo(&output)
	if err != nil {
		t.Fatal(err)
	}
	if written != int64(output.Len()) {
		t.Fatalf("written bytes = %d, output len = %d", written, output.Len())
	}

	want := strings.Join([]string{
		"# HELP svc_queue_depth svc_queue_depth",
		"# TYPE svc_queue_depth gauge",
		`svc_queue_depth{queue="jobs\"fast\\lane\nprimary"} 5`,
		"# HELP svc_request_duration_ms Request duration",
		"# TYPE svc_request_duration_ms histogram",
		`svc_request_duration_ms_bucket{route="/readyz",le="50"} 0`,
		`svc_request_duration_ms_bucket{route="/readyz",le="100"} 1`,
		`svc_request_duration_ms_bucket{route="/readyz",le="+Inf"} 2`,
		`svc_request_duration_ms_sum{route="/readyz"} 200`,
		`svc_request_duration_ms_count{route="/readyz"} 2`,
		"# HELP svc_requests_total HTTP requests by route",
		"# TYPE svc_requests_total counter",
		`svc_requests_total{method="GET",route="/readyz"} 3`,
		"",
	}, "\n")
	if output.String() != want {
		t.Fatalf("exposition mismatch\nwant:\n%s\ngot:\n%s", want, output.String())
	}
}

func TestMeterHandlerServesPrometheusText(t *testing.T) {
	meter, err := New(Config{Namespace: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	meter.Counter("requests_total").Add(context.Background(), 1)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	meter.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	if contentType := recorder.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/plain") {
		t.Fatalf("content type = %q", contentType)
	}
	if !strings.Contains(recorder.Body.String(), "svc_requests_total 1\n") {
		t.Fatalf("body = %q", recorder.Body.String())
	}
}

func TestPushGatewayClientPushesCurrentTextExposition(t *testing.T) {
	meter, err := New(Config{Namespace: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	meter.Counter("requests_total").Add(context.Background(), 4)

	var gotMethod string
	var gotPath string
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.EscapedPath()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		gotBody = string(body)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := NewPushGateway(PushGatewayConfig{
		URL:    server.URL,
		Job:    "checkout worker",
		Labels: map[string]string{"instance": "pod/1"},
	})
	if err := client.Push(context.Background(), meter); err != nil {
		t.Fatal(err)
	}

	if gotMethod != http.MethodPut {
		t.Fatalf("method = %s", gotMethod)
	}
	if gotPath != "/metrics/job/checkout%20worker/instance/pod%2F1" {
		t.Fatalf("path = %s", gotPath)
	}
	if !strings.Contains(gotBody, "svc_requests_total 4\n") {
		t.Fatalf("body = %q", gotBody)
	}
}

func TestSQLHookRecordsQueryMetricsFromCapMetadata(t *testing.T) {
	meter, err := New(Config{Namespace: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	hook := SQLHook(meter)

	ctx := hook.BeforeQuery(context.Background(), capsql.QueryMetadata{
		Name:      "primary",
		Driver:    "mysql",
		Operation: "select",
		Target:    "orders",
	})
	hook.AfterQuery(ctx, capsql.QueryMetadata{
		Name:      "primary",
		Driver:    "mysql",
		Operation: "select",
		Target:    "orders",
		Duration:  25 * time.Millisecond,
	})
	hook.AfterQuery(ctx, capsql.QueryMetadata{
		Name:      "primary",
		Driver:    "mysql",
		Operation: "insert",
		Target:    "orders",
		Duration:  10 * time.Millisecond,
		Err:       errors.New("duplicate"),
	})

	snapshot := meter.Snapshot()
	if snapshot[`svc_sql_queries_total{db="primary",driver="mysql",operation="select",status="ok",target="orders"}`] != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if snapshot[`svc_sql_queries_total{db="primary",driver="mysql",operation="insert",status="error",target="orders"}`] != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if snapshot[`svc_sql_query_duration_seconds_count{db="primary",driver="mysql",operation="select",status="ok",target="orders"}`] != 1 {
		t.Fatalf("duration count = %#v", snapshot)
	}
}

func TestRedisHookRecordsOperationMetricsFromCapMetadata(t *testing.T) {
	meter, err := New(Config{Namespace: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	hook := RedisHook(meter)

	ctx := hook.BeforeRedis(context.Background(), capredis.OperationEvent{Name: "GET", CommandCount: 1})
	hook.AfterRedis(ctx, capredis.OperationEvent{Name: "GET", CommandCount: 1, Duration: 5 * time.Millisecond})
	hook.AfterRedis(ctx, capredis.OperationEvent{Name: "PIPELINE", CommandCount: 3, Err: errors.New("boom")})

	snapshot := meter.Snapshot()
	if snapshot[`svc_redis_commands_total{command="GET",status="ok"}`] != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if snapshot[`svc_redis_commands_total{command="PIPELINE",status="error"}`] != 3 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if snapshot[`svc_redis_command_duration_seconds_count{command="GET",status="ok"}`] != 1 {
		t.Fatalf("duration count = %#v", snapshot)
	}
}

func TestMQObserverRecordsProducerConsumerAndErrorMetadata(t *testing.T) {
	meter, err := New(Config{Namespace: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	observer := MQObserver(meter)
	message := capmq.Message{
		Topic: "orders",
		Metadata: capmq.Metadata{
			Group:           "billing",
			Partition:       2,
			DeliveryAttempt: 3,
		},
	}

	observer.ProducerCallback().OnSuccess(context.Background(), message, capmq.PublishResult{Topic: "orders", Partition: 2})
	observer.RecordDelivery(context.Background(), message)
	observer.RecordDecision(context.Background(), message, capmq.Decision{Action: capmq.DecisionDeadLetter}, errors.New("handler failed"))
	observer.ErrorHandler().HandleConsumerError(context.Background(), errors.New("rebalance"), message.Metadata)

	snapshot := meter.Snapshot()
	if snapshot[`svc_mq_messages_total{group="billing",operation="publish",status="ok",topic="orders"}`] != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if snapshot[`svc_mq_messages_total{group="billing",operation="delivery",status="ok",topic="orders"}`] != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if snapshot[`svc_mq_messages_total{group="billing",operation="dead_letter",status="error",topic="orders"}`] != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if snapshot[`svc_mq_consumer_errors_total{group="billing",topic=""}`] != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if snapshot[`svc_mq_delivery_attempts{group="billing",partition="2",topic="orders"}`] != 3 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func findSeries(t *testing.T, series []capmetric.Series, kind capmetric.InstrumentKind, name string) capmetric.Series {
	t.Helper()
	for _, candidate := range series {
		if candidate.Descriptor.Kind == kind && candidate.Descriptor.Name == name {
			return candidate
		}
	}
	t.Fatalf("series %s %s not found in %#v", kind, name, series)
	return capmetric.Series{}
}
