package file

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	capconfig "github.com/nucleuskit/cap/config"
	caphealth "github.com/nucleuskit/cap/health"
)

func TestFileLoaderImplementsConfigCapability(t *testing.T) {
	var _ capconfig.Loader = (*Loader)(nil)
	var _ capconfig.Scanner = (*Loader)(nil)
	var _ capconfig.Watcher = (*Loader)(nil)

	path := filepath.Join(t.TempDir(), "app.yaml")
	if err := os.WriteFile(path, []byte("service:\n  name: demo\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	loader, err := New(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loader.Close() }()

	values, err := loader.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	serviceName, ok := lookupNestedString(values["service"], "name")
	if !ok {
		t.Fatalf("expected service name, got %#v", values["service"])
	}
	if serviceName != "demo" {
		t.Fatalf("expected service name demo, got %#v", serviceName)
	}
}

func TestFileLoaderDecodesJSONTOMLAndJSON5(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		format Format
		body   string
	}{
		{
			name:   "json",
			path:   "app.json",
			format: FormatJSON,
			body:   `{"service":{"name":"json-demo"},"port":8080}`,
		},
		{
			name:   "toml",
			path:   "app.toml",
			format: FormatTOML,
			body:   "[service]\nname = \"toml-demo\"\nport = 8080\n",
		},
		{
			name:   "json5",
			path:   "app.json5",
			format: FormatJSON5,
			body:   "{// comment\nservice:{name:'json5-demo',},\n}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tt.path)
			if err := os.WriteFile(path, []byte(tt.body), 0o644); err != nil {
				t.Fatal(err)
			}
			loader, err := New(Config{Path: path, Format: tt.format})
			if err != nil {
				t.Fatal(err)
			}
			values, err := loader.Load(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			serviceName, ok := lookupNestedString(values["service"], "name")
			if !ok || !strings.HasPrefix(serviceName, tt.name) {
				t.Fatalf("expected %s service name, got %#v", tt.name, values["service"])
			}
		})
	}
}

func TestFileLoaderRejectsMissingPath(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected missing path error")
	}
}

func TestFileLoaderWritesCacheAndFallsBackWhenMainFileIsMissing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.yaml")
	cachePath := filepath.Join(dir, "cache", "app.yaml")
	if err := os.WriteFile(path, []byte("service:\n  name: primary\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	loader, err := New(Config{Path: path, CachePath: cachePath, Fallback: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loader.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatalf("expected cache to be written: %v", err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	values, err := loader.Load(context.Background())
	if err != nil {
		t.Fatalf("expected cache fallback, got %v", err)
	}
	serviceName, ok := lookupNestedString(values["service"], "name")
	if !ok || serviceName != "primary" {
		t.Fatalf("expected fallback service name primary, got %#v", values["service"])
	}
}

func TestFileLoaderGeneratesTemplateFromMainFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.yaml")
	templatePath := filepath.Join(dir, "templates", "app.yaml")
	if err := os.WriteFile(path, []byte("service:\n  name: templated\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	loader, err := New(Config{Path: path, TemplatePath: templatePath})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loader.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatalf("expected template to be generated: %v", err)
	}
	if string(data) != "service:\n  name: templated\n" {
		t.Fatalf("unexpected template content: %q", data)
	}
}

func TestFileLoaderWatchReportsRevisionAndSortedSources(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.yaml")
	cachePath := filepath.Join(dir, "cache.yaml")
	if err := os.WriteFile(path, []byte("service:\n  name: watched\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	loader, err := New(Config{
		Path:          path,
		Source:        "local-file",
		Priority:      20,
		CachePath:     cachePath,
		CachePriority: 80,
		Fallback:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	updates, err := loader.Watch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	update, ok := <-updates
	if !ok {
		t.Fatal("expected initial update")
	}
	if update.Source != "local-file" || update.Revision == "" {
		t.Fatalf("expected source and revision, got %#v", update)
	}

	sources := loader.Sources()
	if len(sources) != 2 {
		t.Fatalf("expected main and cache sources, got %#v", sources)
	}
	if sources[0].Name != "local-file" || sources[1].Name != "local-file-cache" {
		t.Fatalf("expected sources sorted by priority, got %#v", sources)
	}
}

func TestFileLoaderReportsConfigHealth(t *testing.T) {
	var _ caphealth.Reporter = (*Loader)(nil)

	dir := t.TempDir()
	path := filepath.Join(dir, "app.yaml")
	if err := os.WriteFile(path, []byte("service:\n  name: healthy\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	loader, err := New(Config{Path: path, Source: "local-file"})
	if err != nil {
		t.Fatal(err)
	}
	report, err := loader.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Capability != "config" || report.Status != caphealth.StatusReady {
		t.Fatalf("expected ready config report, got %#v", report)
	}
	if report.Metadata["provider"] != "file" || report.Metadata["source"] != "local-file" || report.Metadata["path"] != path {
		t.Fatalf("unexpected health metadata: %#v", report.Metadata)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	report, err = loader.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDown || report.Message == "" {
		t.Fatalf("expected missing file to report down with message, got %#v", report)
	}
}

func lookupNestedString(value any, key string) (string, bool) {
	switch typed := value.(type) {
	case capconfig.Values:
		v, ok := typed[key].(string)
		return v, ok
	case map[string]any:
		v, ok := typed[key].(string)
		return v, ok
	default:
		return "", false
	}
}
