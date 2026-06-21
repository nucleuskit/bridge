package nacosofficial

import (
	"errors"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	bridgenacos "github.com/nucleuskit/bridge/nacos"
)

var (
	ErrMissingFactory = errors.New("nacos official config client factory is required")
	ErrMissingServer  = errors.New("nacos official server config is required")
)

type NacosClientParam = vo.NacosClientParam

type ConfigClientFactory func(NacosClientParam) (bridgenacos.ConfigClient, error)

func New(cfg bridgenacos.SDKConfig) (*bridgenacos.SDKConfigProvider, error) {
	return NewWithFactory(cfg, newOfficialConfigClient)
}

func NewWithFactory(cfg bridgenacos.SDKConfig, factory ConfigClientFactory) (*bridgenacos.SDKConfigProvider, error) {
	if factory == nil {
		return nil, ErrMissingFactory
	}
	if len(cfg.Servers) == 0 {
		return nil, ErrMissingServer
	}
	client, err := factory(nacosClientParam(cfg))
	if err != nil {
		return nil, err
	}
	return bridgenacos.NewConfigClientProvider(client, cfg)
}

func newOfficialConfigClient(param NacosClientParam) (bridgenacos.ConfigClient, error) {
	client, err := clients.NewConfigClient(param)
	if err != nil {
		return nil, err
	}
	return officialConfigClient{client: client}, nil
}

func nacosClientParam(cfg bridgenacos.SDKConfig) NacosClientParam {
	return vo.NacosClientParam{
		ClientConfig:  clientConfig(cfg),
		ServerConfigs: serverConfigs(cfg.Servers),
	}
}

func clientConfig(cfg bridgenacos.SDKConfig) *constant.ClientConfig {
	return &constant.ClientConfig{
		NamespaceId:         cfg.NamespaceID,
		TimeoutMs:           cfg.TimeoutMs,
		CacheDir:            cfg.CacheDir,
		LogDir:              cfg.LogDir,
		LogLevel:            cfg.LogLevel,
		Username:            cfg.Username,
		Password:            cfg.Password,
		ContextPath:         cfg.ContextPath,
		DisableUseSnapShot:  cfg.DisableUseSnapshot,
		NotLoadCacheAtStart: cfg.NotLoadCacheAtStart,
	}
}

func serverConfigs(servers []bridgenacos.SDKServer) []constant.ServerConfig {
	out := make([]constant.ServerConfig, 0, len(servers))
	for _, server := range servers {
		out = append(out, constant.ServerConfig{
			IpAddr:      server.IP,
			Port:        server.Port,
			GrpcPort:    server.GRPCPort,
			Scheme:      firstNonEmpty(server.Scheme, "http"),
			ContextPath: firstNonEmpty(server.ContextPath, "/nacos"),
		})
	}
	return out
}

type officialConfigClient struct {
	client officialSDKClient
}

func (c officialConfigClient) GetConfig(param bridgenacos.ConfigParam) (string, error) {
	return c.client.GetConfig(toSDKConfigParam(param))
}

func (c officialConfigClient) ListenConfig(param bridgenacos.ConfigParam) error {
	return c.client.ListenConfig(toSDKConfigParam(param))
}

func (c officialConfigClient) CancelListenConfig(param bridgenacos.ConfigParam) error {
	return c.client.CancelListenConfig(toSDKConfigParam(param))
}

func (c officialConfigClient) CloseClient() {
	c.client.CloseClient()
}

type officialSDKClient interface {
	GetConfig(vo.ConfigParam) (string, error)
	ListenConfig(vo.ConfigParam) error
	CancelListenConfig(vo.ConfigParam) error
	CloseClient()
}

func toSDKConfigParam(param bridgenacos.ConfigParam) vo.ConfigParam {
	return vo.ConfigParam{
		DataId:   param.DataID,
		Group:    param.Group,
		OnChange: param.OnChange,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
