package delay

import (
	"context"
	"time"
)

type Queue interface {
	Add(ctx context.Context, taskID string, executeAt time.Time) error    // ZADD
	Poll(ctx context.Context, now time.Time, limit int) ([]string, error) // ZRANGEBYSCORE + ZREM
	Remove(ctx context.Context, taskID string) error                      // ZREM
}

func Add(ctx context.Context, q Queue, taskID string, executeAt time.Time) error {
	panic("implement me")
}

func Poll(ctx context.Context, q Queue, now time.Time, limit int) ([]string, error) {
	panic("implement me")
}

func Remove(ctx context.Context, q Queue, taskID string) error {
	panic("implement me")
}
