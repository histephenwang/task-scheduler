package redis

import (
	"context"

	"github.com/histephenwang/task-scheduler/internal/config"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	*redis.Client
}

func NewClient(cfg *config.RedisConfig) *redis.Client {
	cli := redis.NewClient(&redis.Options{
		Addr:     cfg.Host,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	return cli
}

func (c *Client) Close() error {
	return c.Client.Close()
}

func (c *Client) Ping() error {
	return c.Client.Ping(context.Background()).Err()
}

func (c *Client) get(ctx context.Context, key string) string {
	val, err := c.Client.Get(ctx, key).Result()
	if err != nil {
		return ""
	}
	return val
}

func (c *Client) set(ctx context.Context, key string, value interface{}) error {
	return c.Client.Set(ctx, key, value, 0).Err()
}

func (c *Client) del(ctx context.Context, key string) error {
	return c.Client.Del(ctx, key).Err()
}

func (c *Client) exists(ctx context.Context, key string) (bool, error) {
	res, err := c.Client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (c *Client) incr(ctx context.Context, key string) (int64, error) {
	return c.Client.Incr(ctx, key).Result()
}

func (c *Client) decr(ctx context.Context, key string) (int64, error) {
	return c.Client.Decr(ctx, key).Result()
}
