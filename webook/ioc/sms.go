package ioc

import (
	"github.com/hoysics/geektime-basic-go/webook/internal/service/sms"
	"github.com/hoysics/geektime-basic-go/webook/internal/service/sms/memory"
)

func InitSMSService() sms.Service {
	// 换内存，还是换别的
	return memory.NewService()
}
