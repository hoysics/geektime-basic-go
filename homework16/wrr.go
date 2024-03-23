package wrr

import (
	"context"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"sync"
)

const name = "custom_wrr"

// balancer.Balancer 接口
// balancer.Builder 接口
// balancer.Picker 接口
// base.PickerBuilder 接口
// 你可以认为，Balancer 是 Picker 的装饰器
func init() {
	// NewBalancerBuilder 是帮我们把一个 Picker Builder 转化为一个 balancer.Builder
	balancer.Register(base.NewBalancerBuilder("custom_wrr",
		&PickerBuilder{}, base.Config{HealthCheck: false}))
}

// 传统版本的基于权重的负载均衡算法

type PickerBuilder struct {
}

func (p *PickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	conns := make([]*conn, 0, len(info.ReadySCs))
	// sc => SubConn
	// sci => SubConnInfo
	for sc, sci := range info.ReadySCs {
		cc := &conn{
			cc: sc,
		}
		md, ok := sci.Address.Metadata.(map[string]any)
		if ok {
			weightVal := md["weight"]
			weight, _ := weightVal.(float64)
			cc.weight = int(weight)
			//group, _ := md["group"]
			//cc.group =group
		}

		if cc.weight == 0 {
			// 可以给个默认值
			cc.weight = 10
		}
		cc.currentWeight = cc.weight
		conns = append(conns, cc)
	}
	return &Picker{
		conns: conns,
	}
}

type Picker struct {
	//	 这个才是真的执行负载均衡的地方
	conns     []*conn
	mutex     sync.Mutex
	threshold int // 阈值
}

// Pick 在这里实现基于权重的负载均衡算法
func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if len(p.conns) == 0 {
		// 没有候选节点
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	var total int
	var maxCC *conn
	for _, cc := range p.conns {
		if !cc.available {
			continue
		}
		total += cc.weight
		cc.currentWeight = cc.currentWeight + cc.weight
		if maxCC == nil || cc.currentWeight > maxCC.currentWeight {
			maxCC = cc
		}
	}

	// 更新
	maxCC.currentWeight = maxCC.currentWeight - total
	// maxCC 就是挑出来的
	return balancer.PickResult{
		SubConn: maxCC.cc,
		Done: func(info balancer.DoneInfo) {
			// 很多动态算法，根据调用结果来调整权重，就在这里
			err := info.Err
			if err == nil {
				// 你可以考虑增加权重 weight/currentWeight
				return
			}
			switch err {
			// 一般是主动取消，你没必要去调
			case context.Canceled:
				return
			case context.DeadlineExceeded:
			case io.EOF, io.ErrUnexpectedEOF:
				maxCC.available = true
			// 可以考虑降低权重
			default:
				st, ok := status.FromError(err)
				if ok {
					code := st.Code()
					switch code {
					case codes.Unavailable:
						maxCC.available = false
						go func() {
							if p.healthCheck(maxCC) {
								maxCC.available = true
							}
						}()
					case codes.ResourceExhausted:
					}
				}
			}
		},
	}, nil
}

func (p *Picker) PickAndAdjust(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if len(p.conns) == 0 {
		// 没有候选节点
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	var total int
	var maxCC *conn
	for _, cc := range p.conns {
		if !cc.available {
			continue
		}
		total += cc.weight
		cc.currentWeight = cc.currentWeight + cc.weight
		if maxCC == nil || cc.currentWeight > maxCC.currentWeight {
			maxCC = cc
		}
	}

	// 更新
	maxCC.currentWeight = maxCC.currentWeight - total

	// 调整权重
	if maxCC.currentWeight < p.threshold {
		maxCC.currentWeight = p.threshold
	} else if maxCC.currentWeight > 100 { // 假设最大权重为100
		maxCC.currentWeight = 100
	}

	// maxCC 就是挑出来的
	return balancer.PickResult{
		SubConn: maxCC.cc,
		Done: func(info balancer.DoneInfo) {
			// 根据调用结果调整权重
			err := info.Err
			if err == nil {
				// 提高权重
				maxCC.currentWeight += 10 // 假设每次增加10
			} else {
				// 降低权重
				maxCC.currentWeight -= 10 // 假设每次减少10
				if maxCC.currentWeight < p.threshold {
					maxCC.currentWeight = p.threshold
				}
			}
		},
	}, nil
}

func (p *Picker) healthCheck(cc *conn) bool {
	// 调用 grpc 内置的那个 health check 接口
	return true
}

// conn 代表节点
type conn struct {
	// （初始）权重
	weight int
	// 有效权重
	//efficientWeight int
	currentWeight int

	//lock sync.Mutex

	//	真正的，grpc 里面的代表一个节点的表达
	cc balancer.SubConn

	available bool

	// 假如有 vip 或者非 vip
	group string
}
