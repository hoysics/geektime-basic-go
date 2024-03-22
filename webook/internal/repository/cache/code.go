package cache

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"github.com/dgraph-io/ristretto"
	"github.com/redis/go-redis/v9"
	"time"
)

type CodeCache interface {
	Set(ctx context.Context, biz, phone, code string) error
	Verify(ctx context.Context, biz, phone, inputCode string) (bool, error)
}

// LocalCodeCache 假如说你要切 换这个，你是不是得把 lua 脚本的逻辑，在这里再写一遍？
type LocalCodeCache struct {
	cache *ristretto.Cache
}

func NewCodeCache(client redis.Cmdable) CodeCache {
	cache, _ := ristretto.NewCache(&ristretto.Config{
		NumCounters:        100,
		MaxCost:            10,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	return &LocalCodeCache{
		cache: cache,
	}
}

func (c *LocalCodeCache) Set(ctx context.Context, biz, phone, code string) error {
	k := c.key(biz, phone)
	if _, ok := c.cache.Get(k); ok {
		return ErrCodeSendTooMany
	}
	if ok := c.cache.SetWithTTL(k, code, 1, 10*time.Minute); !ok {
		return errors.New("系统错误")
	}
	return nil
}

func (c *LocalCodeCache) Verify(ctx context.Context, biz, phone, inputCode string) (bool, error) {
	k := c.key(biz, phone)
	var val interface{}
	var ok bool
	if val, ok = c.cache.Get(k); !ok {
		return false, ErrUnknownForCode
	}
	var scode string
	if scode, ok = val.(string); !ok || scode == "" {
		return false, ErrUnknownForCode
	}
	if scode != inputCode {
		return false, ErrUnknownForCode
	}
	c.cache.Del(k)
	return true, nil
}

func (c *LocalCodeCache) key(biz, phone string) string {
	return fmt.Sprintf("phone_code:%s:%s", biz, phone)
}
