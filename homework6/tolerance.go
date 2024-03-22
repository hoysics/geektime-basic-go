package homework6

import (
	"time"
)

type FaultToleranceMiddleware struct {
	checkLimit    func(Request) bool // 判断是否触发限流的函数
	checkCrash    func() bool        // 判断服务商是否崩溃的函数
	saveToDB      func(Request)      // 将请求转储到数据库的函数
	AsyncRetry    int                // 异步重试次数
	RetryInterval time.Duration      // 重试间隔
}

type Request struct {
	Data interface{}
}

func NewFaultToleranceMiddleware(
	limit func(Request) bool,
	crash func() bool,
	dump func(Request),
	retry int,
	interval time.Duration,
) *FaultToleranceMiddleware {
	return &FaultToleranceMiddleware{
		checkLimit:    limit,
		checkCrash:    crash,
		saveToDB:      dump,
		AsyncRetry:    retry,
		RetryInterval: interval,
	}
}

func (ftm *FaultToleranceMiddleware) HandleRequest(req Request, next func(Request)) {
	if ftm.shouldDumpToDB(req) {
		go ftm.asyncSend(req, next)
		ftm.saveToDB(req)
	} else {
		next(req)
	}
}

func (ftm *FaultToleranceMiddleware) shouldDumpToDB(req Request) bool {
	return ftm.checkLimit(req) || ftm.checkCrash()
}

func (ftm *FaultToleranceMiddleware) asyncSend(req Request, next func(Request)) {
	for i := 0; i < ftm.AsyncRetry; i++ {
		time.Sleep(ftm.RetryInterval)
		if !ftm.checkCrash() {
			next(req)
			break
		}
	}
}
