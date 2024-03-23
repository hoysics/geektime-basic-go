package startup

import (
	"github.com/hoysics/geektime-basic-go/homework7/internal/service/oauth2/wechat"
	"github.com/hoysics/geektime-basic-go/homework7/pkg/logger"
)

// InitPhantomWechatService 没啥用的虚拟的 wechatService
func InitPhantomWechatService(l logger.LoggerV1) wechat.Service {
	return wechat.NewService("", "", l)
}
