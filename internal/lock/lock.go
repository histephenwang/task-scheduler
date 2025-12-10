package lock

import (
	"context"
	"errors"
	"time"
)

var (
	ErrLockNotHeld   = errors.New("lock not held")
	ErrLockNotObtain = errors.New("failed to obtain lock")
)

// DistributedLock 分布式锁接口
type DistributedLock interface {
	// TryLock 尝试获取锁，立即返回
	TryLock(ctx context.Context, key string, ttl time.Duration) (bool, error)

	// Lock 阻塞获取锁，直到成功或超时
	Lock(ctx context.Context, key string, ttl time.Duration, waitTimeout time.Duration) (bool, error)

	// Unlock 释放锁
	Unlock(ctx context.Context, key string) error

	// Renew 续期
	Renew(ctx context.Context, key string, ttl time.Duration) (bool, error)
}
