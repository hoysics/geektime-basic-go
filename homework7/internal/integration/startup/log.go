package startup

import (
	"github.com/hoysics/geektime-basic-go/homework7/pkg/logger"
)

func InitLog() logger.LoggerV1 {
	return &logger.NopLogger{}
}
