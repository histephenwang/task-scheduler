package lock

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	unlockScript = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`

	renewScript = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`
)

type redisLock struct {
	client   *redis.Client
	identity string // 当前实例唯一标识
}

func NewRedisLock(client *redis.Client, identity string) DistributedLock {
	return &redisLock{
		client:   client,
		identity: identity,
	}
}

func (l *redisLock) TryLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return l.client.SetNX(ctx, l.buildKey(key), l.identity, ttl).Result()
}

func (l *redisLock) Lock(ctx context.Context, key string, ttl time.Duration, waitTimeout time.Duration) (bool, error) {
	deadline := time.Now().Add(waitTimeout)
	retryInterval := 50 * time.Millisecond

	for {
		acquired, err := l.TryLock(ctx, key, ttl)
		if err != nil {
			return false, err
		}
		if acquired {
			return true, nil
		}

		// 超时检查
		if time.Now().After(deadline) {
			return false, nil
		}

		// 等待重试
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(retryInterval):
			// 指数退避，最大 500ms
			retryInterval = min(retryInterval*2, 500*time.Millisecond)
		}
	}
}

func (l *redisLock) Unlock(ctx context.Context, key string) error {
	result, err := l.client.Eval(ctx, unlockScript, []string{l.buildKey(key)}, l.identity).Int64()
	if err != nil {
		return err
	}
	if result == 0 {
		return ErrLockNotHeld
	}
	return nil
}

func (l *redisLock) Renew(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	result, err := l.client.Eval(ctx, renewScript, []string{l.buildKey(key)}, l.identity, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (l *redisLock) buildKey(key string) string {
	return "lock:" + key
}
