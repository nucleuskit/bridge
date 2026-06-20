package prometheus

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	capmetric "github.com/nucleuskit/nucleus/cap/metric"
)

type Config struct {
	Namespace string
}

type Meter struct {
	namespace string

	mu     sync.Mutex
	series map[string]*seriesState
}

type counter struct {
	meter *Meter
	desc  capmetric.Descriptor
}

type gauge struct {
	meter *Meter
	desc  capmetric.Descriptor
}

type histogram struct {
	meter *Meter
	desc  capmetric.Descriptor
}

type seriesState struct {
	descriptor capmetric.Descriptor
	labels     capmetric.LabelSet
	value      float64
	count      uint64
	sum        float64
	buckets    []capmetric.Bucket
}

func New(cfg Config) (*Meter, error) {
	return &Meter{namespace: cfg.Namespace, series: map[string]*seriesState{}}, nil
}

func (m *Meter) Counter(name string, options ...capmetric.InstrumentOption) capmetric.Counter {
	return counter{meter: m, desc: m.descriptor(capmetric.KindCounter, name, options...)}
}

func (m *Meter) Gauge(name string, options ...capmetric.InstrumentOption) capmetric.Gauge {
	return gauge{meter: m, desc: m.descriptor(capmetric.KindGauge, name, options...)}
}

func (m *Meter) Histogram(name string, options ...capmetric.InstrumentOption) capmetric.Histogram {
	return histogram{meter: m, desc: m.descriptor(capmetric.KindHistogram, name, options...)}
}

func (m *Meter) Snapshot() map[string]float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	values := map[string]float64{}
	for _, state := range m.series {
		switch state.descriptor.Kind {
		case capmetric.KindHistogram:
			values[metricKey(state.descriptor.Name+"_sum", state.labels)] = state.sum
			values[metricKey(state.descriptor.Name+"_count", state.labels)] = float64(state.count)
			for _, bucket := range state.buckets {
				labels := state.labels.Clone()
				if labels == nil {
					labels = capmetric.LabelSet{}
				}
				labels["le"] = formatBucket(bucket.UpperBound)
				values[metricKey(state.descriptor.Name+"_bucket", labels)] = float64(bucket.Count)
			}
		default:
			values[metricKey(state.descriptor.Name, state.labels)] = state.value
		}
	}
	return values
}

func (m *Meter) SnapshotSeries() []capmetric.Series {
	m.mu.Lock()
	defer m.mu.Unlock()
	values := make([]capmetric.Series, 0, len(m.series))
	for _, state := range m.series {
		values = append(values, state.snapshot())
	}
	sort.Slice(values, func(i int, j int) bool {
		left := values[i].Descriptor.Name + values[i].Labels.Key()
		right := values[j].Descriptor.Name + values[j].Labels.Key()
		return left < right
	})
	return values
}

func (m *Meter) WriteTo(w io.Writer) (int64, error) {
	var output strings.Builder
	writePrometheusText(&output, m.SnapshotSeries())
	n, err := io.WriteString(w, output.String())
	return int64(n), err
}

func (m *Meter) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if _, err := m.WriteTo(w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func (m *Meter) Close() error {
	return nil
}

func (m *Meter) key(name string) string {
	if strings.TrimSpace(m.namespace) == "" {
		return name
	}
	return m.namespace + "_" + name
}

func (m *Meter) descriptor(kind capmetric.InstrumentKind, name string, options ...capmetric.InstrumentOption) capmetric.Descriptor {
	values := capmetric.NewInstrumentOptions(options...)
	return capmetric.Descriptor{
		Kind:        kind,
		Name:        m.key(name),
		Unit:        values.Unit,
		Description: values.Description,
		Labels:      append([]string(nil), values.Labels...),
		Buckets:     append([]float64(nil), values.Buckets...),
	}
}

func (c counter) Descriptor() capmetric.Descriptor {
	return c.desc.Clone()
}

func (c counter) Add(ctx context.Context, value float64, attributes ...capmetric.Attribute) {
	c.meter.mu.Lock()
	defer c.meter.mu.Unlock()
	state := c.meter.state(c.desc, attributes...)
	state.value += value
}

func (c counter) Record(ctx context.Context, value float64, attributes ...capmetric.Attribute) {
	c.Add(ctx, value, attributes...)
}

func (g gauge) Descriptor() capmetric.Descriptor {
	return g.desc.Clone()
}

func (g gauge) Set(ctx context.Context, value float64, attributes ...capmetric.Attribute) {
	g.meter.mu.Lock()
	defer g.meter.mu.Unlock()
	state := g.meter.state(g.desc, attributes...)
	state.value = value
}

func (g gauge) Record(ctx context.Context, value float64, attributes ...capmetric.Attribute) {
	g.Set(ctx, value, attributes...)
}

func (h histogram) Descriptor() capmetric.Descriptor {
	return h.desc.Clone()
}

func (h histogram) Observe(ctx context.Context, value float64, attributes ...capmetric.Attribute) {
	h.meter.mu.Lock()
	defer h.meter.mu.Unlock()
	state := h.meter.state(h.desc, attributes...)
	state.value = value
	state.count++
	state.sum += value
	for i := range state.buckets {
		if value <= state.buckets[i].UpperBound || math.IsInf(state.buckets[i].UpperBound, 1) {
			state.buckets[i].Count++
		}
	}
}

func (h histogram) Record(ctx context.Context, value float64, attributes ...capmetric.Attribute) {
	h.Observe(ctx, value, attributes...)
}

func (m *Meter) state(descriptor capmetric.Descriptor, attributes ...capmetric.Attribute) *seriesState {
	labels := capmetric.LabelsFor(descriptor, attributes...)
	key := string(descriptor.Kind) + "|" + descriptor.Name + "|" + labels.Key()
	if state, ok := m.series[key]; ok {
		return state
	}
	state := &seriesState{
		descriptor: descriptor.Clone(),
		labels:     labels.Clone(),
		buckets:    histogramBuckets(descriptor),
	}
	m.series[key] = state
	return state
}

func (s *seriesState) snapshot() capmetric.Series {
	return capmetric.Series{
		Descriptor: s.descriptor.Clone(),
		Labels:     s.labels.Clone(),
		Value:      s.value,
		Count:      s.count,
		Sum:        s.sum,
		Buckets:    append([]capmetric.Bucket(nil), s.buckets...),
	}
}

func histogramBuckets(descriptor capmetric.Descriptor) []capmetric.Bucket {
	if descriptor.Kind != capmetric.KindHistogram {
		return nil
	}
	values := make([]capmetric.Bucket, 0, len(descriptor.Buckets)+1)
	for _, bucket := range descriptor.Buckets {
		values = append(values, capmetric.Bucket{UpperBound: bucket})
	}
	values = append(values, capmetric.Bucket{UpperBound: math.Inf(1)})
	return values
}

func metricKey(name string, labels capmetric.LabelSet) string {
	if key := labels.Key(); key != "" {
		return name + "{" + key + "}"
	}
	return name
}

func formatBucket(value float64) string {
	if math.IsInf(value, 1) {
		return "+Inf"
	}
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%f", value), "0"), ".")
}

func writePrometheusText(output *strings.Builder, series []capmetric.Series) {
	sort.Slice(series, func(i int, j int) bool {
		if series[i].Descriptor.Name != series[j].Descriptor.Name {
			return series[i].Descriptor.Name < series[j].Descriptor.Name
		}
		return labelsKey(series[i]) < labelsKey(series[j])
	})

	writtenFamilies := map[string]struct{}{}
	for _, sample := range series {
		name := sample.Descriptor.Name
		if _, ok := writtenFamilies[name]; !ok {
			description := sample.Descriptor.Description
			if strings.TrimSpace(description) == "" {
				description = name
			}
			output.WriteString("# HELP ")
			output.WriteString(name)
			output.WriteString(" ")
			output.WriteString(escapeHelp(description))
			output.WriteString("\n# TYPE ")
			output.WriteString(name)
			output.WriteString(" ")
			output.WriteString(prometheusType(sample.Descriptor.Kind))
			output.WriteByte('\n')
			writtenFamilies[name] = struct{}{}
		}
		writePrometheusSamples(output, sample)
	}
}

func writePrometheusSamples(output *strings.Builder, sample capmetric.Series) {
	switch sample.Descriptor.Kind {
	case capmetric.KindHistogram:
		for _, bucket := range sample.Buckets {
			output.WriteString(sample.Descriptor.Name)
			output.WriteString("_bucket")
			output.WriteString(formatLabels(sample.Descriptor.Labels, sample.Labels, "le", formatBucket(bucket.UpperBound)))
			output.WriteByte(' ')
			output.WriteString(formatFloat(float64(bucket.Count)))
			output.WriteByte('\n')
		}
		output.WriteString(sample.Descriptor.Name)
		output.WriteString("_sum")
		output.WriteString(formatLabels(sample.Descriptor.Labels, sample.Labels))
		output.WriteByte(' ')
		output.WriteString(formatFloat(sample.Sum))
		output.WriteByte('\n')
		output.WriteString(sample.Descriptor.Name)
		output.WriteString("_count")
		output.WriteString(formatLabels(sample.Descriptor.Labels, sample.Labels))
		output.WriteByte(' ')
		output.WriteString(formatFloat(float64(sample.Count)))
		output.WriteByte('\n')
	default:
		output.WriteString(sample.Descriptor.Name)
		output.WriteString(formatLabels(sample.Descriptor.Labels, sample.Labels))
		output.WriteByte(' ')
		output.WriteString(formatFloat(sample.Value))
		output.WriteByte('\n')
	}
}

func prometheusType(kind capmetric.InstrumentKind) string {
	switch kind {
	case capmetric.KindCounter:
		return "counter"
	case capmetric.KindGauge:
		return "gauge"
	case capmetric.KindHistogram:
		return "histogram"
	default:
		return "untyped"
	}
}

func labelsKey(sample capmetric.Series) string {
	return formatLabels(sample.Descriptor.Labels, sample.Labels)
}

func formatLabels(order []string, labels capmetric.LabelSet, extra ...string) string {
	if len(labels) == 0 && len(extra) == 0 {
		return ""
	}

	seen := map[string]struct{}{}
	parts := make([]string, 0, len(labels)+len(extra)/2)
	for _, key := range order {
		value, ok := labels[key]
		if !ok {
			continue
		}
		parts = append(parts, formatLabel(key, value))
		seen[key] = struct{}{}
	}

	remaining := make([]string, 0, len(labels))
	for key := range labels {
		if _, ok := seen[key]; !ok {
			remaining = append(remaining, key)
		}
	}
	sort.Strings(remaining)
	for _, key := range remaining {
		parts = append(parts, formatLabel(key, labels[key]))
	}

	for i := 0; i+1 < len(extra); i += 2 {
		parts = append(parts, formatLabel(extra[i], extra[i+1]))
	}
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func formatLabel(key string, value string) string {
	return key + "=\"" + escapeLabelValue(value) + "\""
}

func escapeLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

func escapeHelp(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return value
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}
