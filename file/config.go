package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	capconfig "github.com/nucleuskit/cap/config"
	caphealth "github.com/nucleuskit/cap/health"
	"gopkg.in/yaml.v3"
)

type Format string

const (
	FormatAuto  Format = ""
	FormatYAML  Format = "yaml"
	FormatJSON  Format = "json"
	FormatTOML  Format = "toml"
	FormatJSON5 Format = "json5"
)

type Config struct {
	Path          string
	Source        string
	Priority      int
	CachePath     string
	CachePriority int
	TemplatePath  string
	Fallback      bool
	Format        Format
}

type Loader struct {
	mu            sync.RWMutex
	path          string
	source        string
	priority      int
	cachePath     string
	cachePriority int
	templatePath  string
	fallback      bool
	format        Format
	closed        bool
}

func New(cfg Config) (*Loader, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("file config path is required")
	}
	source := cfg.Source
	if source == "" {
		source = "file"
	}
	cachePriority := cfg.CachePriority
	if cachePriority == 0 {
		cachePriority = cfg.Priority + 100
	}
	return &Loader{
		path:          cfg.Path,
		source:        source,
		priority:      cfg.Priority,
		cachePath:     cfg.CachePath,
		cachePriority: cachePriority,
		templatePath:  cfg.TemplatePath,
		fallback:      cfg.Fallback,
		format:        cfg.Format,
	}, nil
}

func (l *Loader) Load(ctx context.Context) (capconfig.Values, error) {
	values, _, _, err := l.loadValues(ctx)
	if err != nil {
		return nil, err
	}
	if values == nil {
		return capconfig.Values{}, nil
	}
	return values, nil
}

func (l *Loader) Scan(ctx context.Context, target any) error {
	data, _, _, err := l.loadDocument(ctx)
	if err != nil {
		return err
	}
	if err := decodeDocument(data, l.documentFormat(), target); err != nil {
		return err
	}
	return nil
}

func (l *Loader) Watch(ctx context.Context) (<-chan capconfig.Update, error) {
	ch := make(chan capconfig.Update, 1)
	values, source, revision, err := l.loadValues(ctx)
	if err != nil {
		return nil, err
	}
	ch <- capconfig.Update{Values: values, Source: source, Revision: revision}
	close(ch)
	return ch, nil
}

func (l *Loader) Sources() []capconfig.Source {
	sources := []capconfig.Source{{
		Name:     l.source,
		Kind:     "file",
		Location: l.path,
		Priority: l.priority,
	}}
	if l.cachePath != "" && l.fallback {
		sources = append(sources, capconfig.Source{
			Name:     l.cacheSource(),
			Kind:     "cache",
			Location: l.cachePath,
			Priority: l.cachePriority,
		})
	}
	return capconfig.SortSources(sources)
}

func (l *Loader) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	return nil
}

func (l *Loader) ReportHealth(ctx context.Context) (caphealth.Report, error) {
	report := caphealth.Report{
		Capability: "config",
		Status:     caphealth.StatusReady,
		Message:    "file config ready",
		Metadata: map[string]string{
			"provider": "file",
			"source":   l.source,
			"path":     l.path,
		},
	}
	l.mu.RLock()
	closed := l.closed
	l.mu.RUnlock()
	if closed {
		report.Status = caphealth.StatusDown
		report.Message = "file config loader is closed"
		return report, nil
	}
	_, source, revision, err := l.loadDocument(ctx)
	if revision != "" {
		report.Metadata["revision"] = revision
	}
	if source != "" {
		report.Metadata["active_source"] = source
	}
	if err != nil {
		report.Status = caphealth.StatusDown
		report.Message = err.Error()
		return report, nil
	}
	if source == l.cacheSource() {
		report.Status = caphealth.StatusDegraded
		report.Message = "file config using cache fallback"
	}
	return report, nil
}

func (l *Loader) loadValues(ctx context.Context) (capconfig.Values, string, string, error) {
	data, source, revision, err := l.loadDocument(ctx)
	if err != nil {
		return nil, "", "", err
	}
	var values capconfig.Values
	if err := decodeDocument(data, l.documentFormat(), &values); err != nil {
		return nil, "", "", err
	}
	if values == nil {
		values = capconfig.Values{}
	}
	return values, source, revision, nil
}

func (l *Loader) loadDocument(ctx context.Context) ([]byte, string, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", "", err
	}
	data, err := os.ReadFile(l.path)
	if err == nil {
		if err := l.writeCache(data); err != nil {
			return nil, "", "", err
		}
		if err := l.writeTemplate(data); err != nil {
			return nil, "", "", err
		}
		return data, l.source, revisionFor(data), nil
	}
	if !l.fallback || l.cachePath == "" {
		return nil, "", "", err
	}
	cacheData, cacheErr := os.ReadFile(l.cachePath)
	if cacheErr != nil {
		return nil, "", "", fmt.Errorf("read config %s failed: %w; cache fallback %s failed: %v", l.path, err, l.cachePath, cacheErr)
	}
	return cacheData, l.cacheSource(), revisionFor(cacheData), nil
}

func (l *Loader) writeCache(data []byte) error {
	if l.cachePath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(l.cachePath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(l.cachePath, data, 0o644)
}

func (l *Loader) writeTemplate(data []byte) error {
	if l.templatePath == "" {
		return nil
	}
	if _, err := os.Stat(l.templatePath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(l.templatePath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(l.templatePath, data, 0o644)
}

func (l *Loader) cacheSource() string {
	return l.source + "-cache"
}

func (l *Loader) documentFormat() Format {
	if l.format != FormatAuto {
		return l.format
	}
	return formatFromPath(l.path)
}

func formatFromPath(path string) Format {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		return FormatJSON
	case ".toml":
		return FormatTOML
	case ".json5":
		return FormatJSON5
	default:
		return FormatYAML
	}
}

func decodeDocument(data []byte, format Format, target any) error {
	switch format {
	case FormatJSON:
		return json.Unmarshal(data, target)
	case FormatTOML:
		values, err := parseTOML(data)
		if err != nil {
			return err
		}
		return assignDecodedValues(values, target)
	case FormatJSON5:
		normalized, err := normalizeJSON5(data)
		if err != nil {
			return err
		}
		return json.Unmarshal(normalized, target)
	default:
		return yaml.Unmarshal(data, target)
	}
}

func assignDecodedValues(values capconfig.Values, target any) error {
	switch typed := target.(type) {
	case *capconfig.Values:
		*typed = values
		return nil
	case *map[string]any:
		*typed = map[string]any(values)
		return nil
	default:
		data, err := json.Marshal(values)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, target)
	}
}

func parseTOML(data []byte) (capconfig.Values, error) {
	values := capconfig.Values{}
	current := values
	for lineNumber, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(stripComment(raw, '#'))
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			current = values
			for _, part := range strings.Split(strings.Trim(line, "[]"), ".") {
				part = strings.TrimSpace(part)
				if part == "" {
					return nil, fmt.Errorf("invalid toml section on line %d", lineNumber+1)
				}
				next, _ := current[part].(capconfig.Values)
				if next == nil {
					next = capconfig.Values{}
					current[part] = next
				}
				current = next
			}
			continue
		}
		key, rawValue, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("invalid toml assignment on line %d", lineNumber+1)
		}
		current[strings.TrimSpace(key)] = parseScalar(strings.TrimSpace(rawValue))
	}
	return values, nil
}

func parseScalar(value string) any {
	if unquoted, err := strconv.Unquote(value); err == nil {
		return unquoted
	}
	if value == "true" {
		return true
	}
	if value == "false" {
		return false
	}
	if intValue, err := strconv.ParseInt(value, 10, 64); err == nil {
		return intValue
	}
	if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
		return floatValue
	}
	return strings.Trim(value, `"`)
}

func normalizeJSON5(data []byte) ([]byte, error) {
	withoutComments := stripJSON5Comments(string(data))
	quotedKeys := quoteJSON5Keys(withoutComments)
	quotedStrings := strings.ReplaceAll(quotedKeys, "'", `"`)
	withoutTrailingCommas := stripTrailingCommas(quotedStrings)
	if !json.Valid([]byte(withoutTrailingCommas)) {
		return nil, fmt.Errorf("invalid json5 document")
	}
	return []byte(withoutTrailingCommas), nil
}

func stripJSON5Comments(value string) string {
	var builder strings.Builder
	inString := rune(0)
	for i := 0; i < len(value); i++ {
		ch := rune(value[i])
		if inString != 0 {
			builder.WriteByte(value[i])
			if ch == inString && (i == 0 || value[i-1] != '\\') {
				inString = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			inString = ch
			builder.WriteByte(value[i])
			continue
		}
		if ch == '/' && i+1 < len(value) && value[i+1] == '/' {
			for i < len(value) && value[i] != '\n' {
				i++
			}
			if i < len(value) {
				builder.WriteByte(value[i])
			}
			continue
		}
		builder.WriteByte(value[i])
	}
	return builder.String()
}

func quoteJSON5Keys(value string) string {
	var builder strings.Builder
	expectKey := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch == '{' || ch == ',' {
			expectKey = true
			builder.WriteByte(ch)
			continue
		}
		if expectKey && (ch == ' ' || ch == '\n' || ch == '\t' || ch == '\r') {
			builder.WriteByte(ch)
			continue
		}
		if expectKey && isKeyStart(ch) {
			start := i
			for i < len(value) && isKeyPart(value[i]) {
				i++
			}
			key := value[start:i]
			j := i
			for j < len(value) && (value[j] == ' ' || value[j] == '\n' || value[j] == '\t' || value[j] == '\r') {
				j++
			}
			if j < len(value) && value[j] == ':' {
				fmt.Fprintf(&builder, "%q", key)
				i--
				expectKey = false
				continue
			}
			i = start
		}
		if ch == ':' {
			expectKey = false
		}
		builder.WriteByte(ch)
	}
	return builder.String()
}

func stripTrailingCommas(value string) string {
	var builder strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] != ',' {
			builder.WriteByte(value[i])
			continue
		}
		j := i + 1
		for j < len(value) && (value[j] == ' ' || value[j] == '\n' || value[j] == '\t' || value[j] == '\r') {
			j++
		}
		if j < len(value) && (value[j] == '}' || value[j] == ']') {
			continue
		}
		builder.WriteByte(value[i])
	}
	return builder.String()
}

func stripComment(value string, marker byte) string {
	if index := strings.IndexByte(value, marker); index >= 0 {
		return value[:index]
	}
	return value
}

func isKeyStart(ch byte) bool {
	return ch == '_' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z'
}

func isKeyPart(ch byte) bool {
	return isKeyStart(ch) || ch >= '0' && ch <= '9' || ch == '-'
}

func revisionFor(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
