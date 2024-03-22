package homework6

import (
	"testing"
	"time"
)

func TestFaultToleranceMiddleware_HandleRequest(t *testing.T) {
	limitFunc := func(req Request) bool {
		if req.Data == "limit" {
			return true
		}
		return false
	}

	crashFunc := func() bool {
		return time.Now().UnixNano()%2 == 0
	}

	var dumpedReq Request
	dumpFunc := func(req Request) {
		dumpedReq = req
	}

	ftm := NewFaultToleranceMiddleware(limitFunc, crashFunc, dumpFunc, 3, 100*time.Millisecond)

	req := Request{Data: "test"}
	ftm.HandleRequest(req, func(req Request) {})

	if dumpedReq.Data != nil {
		t.Errorf("不该到db")
	}

	reqLimit := Request{Data: "limit"}
	ftm.HandleRequest(reqLimit, func(req Request) {})

	if dumpedReq.Data != "limit" {
		t.Errorf("不该到db")
	}
}
