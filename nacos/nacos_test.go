package nacos

import (
	"context"
	"testing"

	capconfig "github.com/nucleuskit/cap/config"
	capdiscovery "github.com/nucleuskit/cap/discovery"
	caphealth "github.com/nucleuskit/cap/health"
)

func TestProviderImplementsConfigCapabilityWithoutServer(t *testing.T) {
	provider, err := New(Config{
		Namespace: "public",
		Source:    "nacos-public",
		Revision:  "rev-1",
		Values: capconfig.Values{
			"service": map[string]any{"name": "hello-cap"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provider.Close() }()

	var loader capconfig.Loader = provider
	var scanner capconfig.Scanner = provider
	values, err := loader.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if values["service"] == nil {
		t.Fatalf("expected service values, got %#v", values)
	}
	var cfg struct {
		Service struct {
			Name string `yaml:"name"`
		} `yaml:"service"`
	}
	if err := scanner.Scan(context.Background(), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Service.Name != "hello-cap" {
		t.Fatalf("expected hello-cap, got %q", cfg.Service.Name)
	}
}

func TestProviderExposesSourceRevisionAndWatchUpdates(t *testing.T) {
	provider, err := New(Config{
		Namespace: "public",
		Server:    "placeholder://nacos",
		Source:    "nacos-public",
		Priority:  30,
		Revision:  "rev-1",
		Values: capconfig.Values{
			"service": map[string]any{"name": "initial"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provider.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates, err := provider.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	initial := <-updates
	if initial.Source != "nacos-public" || initial.Revision != "rev-1" {
		t.Fatalf("unexpected initial update: %#v", initial)
	}

	if err := provider.Update(context.Background(), capconfig.Values{
		"service": map[string]any{"name": "updated"},
	}, "rev-2"); err != nil {
		t.Fatal(err)
	}
	changed := <-updates
	if changed.Source != "nacos-public" || changed.Revision != "rev-2" {
		t.Fatalf("unexpected changed update: %#v", changed)
	}
	serviceName, ok := changed.Values["service"].(capconfig.Values)["name"].(string)
	if !ok || serviceName != "updated" {
		t.Fatalf("expected updated values, got %#v", changed.Values)
	}

	sources := provider.Sources()
	if len(sources) != 1 {
		t.Fatalf("expected one source, got %#v", sources)
	}
	if sources[0].Name != "nacos-public" || sources[0].Priority != 30 {
		t.Fatalf("unexpected source metadata: %#v", sources)
	}
}

func TestProviderReportsConfigHealth(t *testing.T) {
	var _ caphealth.Reporter = (*Provider)(nil)

	provider, err := New(Config{
		Namespace: "public",
		Server:    "placeholder://nacos",
		Source:    "nacos-public",
		Revision:  "rev-1",
		Values:    capconfig.Values{"feature": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := provider.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Capability != "config" || report.Status != caphealth.StatusReady {
		t.Fatalf("expected ready config report, got %#v", report)
	}
	if report.Metadata["provider"] != "nacos" || report.Metadata["source"] != "nacos-public" || report.Metadata["revision"] != "rev-1" {
		t.Fatalf("unexpected health metadata: %#v", report.Metadata)
	}

	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}
	report, err = provider.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDown || report.Message == "" {
		t.Fatalf("expected closed provider to report down, got %#v", report)
	}
}

func TestProviderExposesStaticDiscoveryProvider(t *testing.T) {
	provider, err := New(Config{
		Namespace: "public",
		Server:    "placeholder://nacos",
		DiscoveryServices: []capdiscovery.Service{{
			Name:      "checkout",
			Namespace: "public",
			Group:     "DEFAULT_GROUP",
			Metadata:  map[string]string{"owner": "platform"},
		}},
		DiscoveryEndpoints: map[string][]capdiscovery.Endpoint{
			"checkout": {{
				Addr:     "127.0.0.1:50051",
				Weight:   10,
				Health:   capdiscovery.HealthServing,
				Topology: capdiscovery.Topology{Region: "cn", Zone: "a"},
				Metadata: map[string]string{"version": "v1"},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provider.Close() }()

	discoveryProvider := provider.Discovery()
	var _ capdiscovery.Provider = discoveryProvider

	endpoints, err := discoveryProvider.Resolve(context.Background(), "checkout")
	if err != nil {
		t.Fatal(err)
	}
	if len(endpoints) != 1 || endpoints[0].Addr != "127.0.0.1:50051" {
		t.Fatalf("unexpected discovery endpoints: %#v", endpoints)
	}
	endpoints[0].Metadata["version"] = "mutated"

	snapshot, err := discoveryProvider.Snapshot(context.Background(), "checkout")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Service != "checkout" || snapshot.Descriptor.Group != "DEFAULT_GROUP" {
		t.Fatalf("unexpected discovery snapshot: %#v", snapshot)
	}
	if snapshot.Endpoints[0].Metadata["version"] != "v1" {
		t.Fatalf("discovery endpoint metadata leaked mutable state: %#v", snapshot)
	}

	updates, err := discoveryProvider.Watch(context.Background(), "checkout")
	if err != nil {
		t.Fatal(err)
	}
	initial := <-updates
	if len(initial.Endpoints) != 1 || initial.Endpoints[0].Weight != 10 {
		t.Fatalf("unexpected discovery update: %#v", initial)
	}
}
