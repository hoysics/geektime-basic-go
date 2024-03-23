package grpc

import (
	"context"
	"fmt"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

var NamingClient naming_client.INamingClient
var ConfigClient config_client.IConfigClient
var ServiceName = "client-go"

type NacosTestSuite struct {
	suite.Suite
	client naming_client.INamingClient
}

func (s *NacosTestSuite) TestNacosClient() {
	//create ServerConfig
	sc := []constant.ServerConfig{
		*constant.NewServerConfig("127.0.0.1", 8848, constant.WithContextPath("/nacos")),
	}

	//create ClientConfig
	cc := *constant.NewClientConfig(
		constant.WithNamespaceId(""),
		constant.WithTimeoutMs(5000),
		constant.WithNotLoadCacheAtStart(true),
		constant.WithLogDir("/tmp/nacos/log"),
		constant.WithCacheDir("/tmp/nacos/cache"),
		constant.WithLogLevel("debug"),
	)

	// create naming client
	client, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  &cc,
			ServerConfigs: sc,
		},
	)
	require.NoError(s.T(), err)
	instance, err := client.SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam{
		ServiceName: "test-server",
		GroupName:   "DEFAULT_GROUP",     // 默认值DEFAULT_GROUP
		Clusters:    []string{"DEFAULT"}, // 默认值DEFAULT
	})
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", instance.Ip, instance.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(s.T(), err)
	defer conn.Close()
	gclient := NewUserServiceClient(conn)
	resp, err := gclient.GetById(context.Background(), &GetByIdRequest{Id: 123})
	require.NoError(s.T(), err)
	s.T().Log(resp.User)
}

func (s *NacosTestSuite) TestNacosServer() {
	//create ServerConfig
	sc := []constant.ServerConfig{
		*constant.NewServerConfig("127.0.0.1", 8848, constant.WithContextPath("/nacos")),
	}

	//create ClientConfig
	cc := *constant.NewClientConfig(
		constant.WithNamespaceId(""),
		constant.WithTimeoutMs(5000),
		constant.WithNotLoadCacheAtStart(true),
		constant.WithLogDir("/tmp/nacos/log"),
		constant.WithCacheDir("/tmp/nacos/cache"),
		constant.WithLogLevel("debug"),
	)

	// create naming client
	client, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  &cc,
			ServerConfigs: sc,
		},
	)
	require.NoError(s.T(), err)
	_, err = client.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          "127.0.0.1",
		Port:        uint64(8090),
		ServiceName: "test-server",
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    map[string]string{"name": "test"},
		ClusterName: "DEFAULT",       // 默认值DEFAULT
		GroupName:   "DEFAULT_GROUP", // 默认值DEFAULT_GROUP
	})
	require.NoError(s.T(), err)
	// Create gRPC server and register the UserService server
	server := grpc.NewServer()
	RegisterUserServiceServer(server, &Server{})
	lis, err := net.Listen("tcp", ":8090")
	require.NoError(s.T(), err)
	go func() {
		err := server.Serve(lis)
		require.NoError(s.T(), err)
	}()
	defer server.Stop()
}

func TestNacosTestSuite(t *testing.T) {
	suite.Run(t, new(NacosTestSuite))
}
