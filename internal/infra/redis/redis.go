package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/histephenwang/task-scheduler/internal/config"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	*redis.Client
}

func NewClient(cfg *config.RedisConfig) *redis.Client {
	cli := redis.NewClient(&redis.Options{
		Addr:         cfg.Host,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		DialTimeout:  time.Duration(cfg.DialTimeout) * time.Second,
		ReadTimeout:  time.Duration(cfg.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.WriteTimeout) * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cli.Ping(ctx).Err(); err != nil {
		panic(fmt.Sprintf("redis ping failed: %v", err))
	}
	return cli
}
