package nacosofficial

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	bridgenacos "github.com/nucleuskit/bridge/nacos"
)

func TestNewUsesOfficialSDKFactoryWithoutLeakingSDKTypesToProviderAPI(t *testing.T) {
	factory := &fakeFactory{}
	provider, err := NewWithFactory(bridgenacos.SDKConfig{
		NamespaceID: "public",
		Servers: []bridgenacos.SDKServer{{
			IP:          "127.0.0.1",
			Port:        8848,
			GRPCPort:    9848,
			Scheme:      "http",
			ContextPath: "/nacos",
		}},
		DataID:              "app.yaml",
		Group:               "APP_CONFIG",
		Source:              "official-nacos",
		TimeoutMs:           3000,
		CacheDir:            "/tmp/nacos/cache",
		LogDir:              "/tmp/nacos/log",
		LogLevel:            "warn",
		Username:            "nacos",
		Password:            "secret",
		DisableUseSnapshot:  true,
		NotLoadCacheAtStart: true,
	}, factory.New)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provider.Close() }()

	if factory.param.ClientConfig == nil {
		t.Fatal("expected client config")
	}
	if factory.param.ClientConfig.NamespaceId != "public" || factory.param.ClientConfig.TimeoutMs != 3000 || factory.param.ClientConfig.CacheDir != "/tmp/nacos/cache" {
		t.Fatalf("unexpected client config: %#v", factory.param.ClientConfig)
	}
	if factory.param.ClientConfig.Username != "nacos" || factory.param.ClientConfig.Password != "secret" {
		t.Fatalf("expected credentials to be passed to SDK config")
	}
	if len(factory.param.ServerConfigs) != 1 || factory.param.ServerConfigs[0].IpAddr != "127.0.0.1" || factory.param.ServerConfigs[0].Port != 8848 || factory.param.ServerConfigs[0].GrpcPort != 9848 {
		t.Fatalf("unexpected server configs: %#v", factory.param.ServerConfigs)
	}

	sources := provider.Sources()
	if len(sources) != 1 || sources[0].Name != "official-nacos" || sources[0].Location != "public/APP_CONFIG/app.yaml" {
		t.Fatalf("unexpected provider sources: %#v", sources)
	}
}

func TestNewValidatesOfficialSDKConfig(t *testing.T) {
	if _, err := NewWithFactory(bridgenacos.SDKConfig{DataID: "app.yaml"}, (&fakeFactory{}).New); err == nil {
		t.Fatal("expected missing server error")
	}
	if _, err := NewWithFactory(bridgenacos.SDKConfig{DataID: "app.yaml", Servers: []bridgenacos.SDKServer{{IP: "127.0.0.1"}}}, nil); err == nil {
		t.Fatal("expected missing factory error")
	}
}

func TestProviderLiveConfigSkipsWithoutAddress(t *testing.T) {
	addr := strings.TrimSpace(os.Getenv("NUCLEUS_NACOS_ADDR"))
	if addr == "" {
		t.Skip("set NUCLEUS_NACOS_ADDR to run live Nacos official config smoke test")
	}
	port := parseUintEnv(t, "NUCLEUS_NACOS_PORT", 8848)
	grpcPort := parseUintEnv(t, "NUCLEUS_NACOS_GRPC_PORT", 9848)
	dataID := firstEnv("NUCLEUS_NACOS_DATA_ID", "app.yaml")
	group := firstEnv("NUCLEUS_NACOS_GROUP", "DEFAULT_GROUP")
	provider, err := New(bridgenacos.SDKConfig{
		NamespaceID: firstEnv("NUCLEUS_NACOS_NAMESPACE", "public"),
		Servers: []bridgenacos.SDKServer{{
			IP:       addr,
			Port:     port,
			GRPCPort: grpcPort,
			Scheme:   firstEnv("NUCLEUS_NACOS_SCHEME", "http"),
		}},
		DataID:    dataID,
		Group:     group,
		Source:    "official-nacos-live",
		Username:  os.Getenv("NUCLEUS_NACOS_USERNAME"),
		Password:  os.Getenv("NUCLEUS_NACOS_PASSWORD"),
		TimeoutMs: 3000,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provider.Close() }()
	report, err := provider.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Capability != "config" || report.Status == "" {
		t.Fatalf("unexpected live report: %#v", report)
	}
	for key, value := range report.Metadata {
		if strings.Contains(strings.ToLower(key), "password") || strings.Contains(value, os.Getenv("NUCLEUS_NACOS_PASSWORD")) && os.Getenv("NUCLEUS_NACOS_PASSWORD") != "" {
			t.Fatalf("health metadata leaked password: %#v", report.Metadata)
		}
	}
}

func parseUintEnv(t *testing.T, name string, fallback uint64) uint64 {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("invalid %s: %v", name, err)
	}
	return value
}

func firstEnv(name string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

type fakeFactory struct {
	param NacosClientParam
}

func (f *fakeFactory) New(param NacosClientParam) (bridgenacos.ConfigClient, error) {
	f.param = param
	return &fakeOfficialConfigClient{content: "ok: true\n"}, nil
}

type fakeOfficialConfigClient struct {
	content string
}

func (f *fakeOfficialConfigClient) GetConfig(bridgenacos.ConfigParam) (string, error) {
	return f.content, nil
}

func (f *fakeOfficialConfigClient) ListenConfig(bridgenacos.ConfigParam) error {
	return nil
}

func (f *fakeOfficialConfigClient) CancelListenConfig(bridgenacos.ConfigParam) error {
	return nil
}

func (f *fakeOfficialConfigClient) CloseClient() {}
