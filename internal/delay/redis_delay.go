package delay

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// Lua: 原子地获取到期任务并删除
	// KEYS[1]: zset key
	// ARGV[1]: 当前时间戳
	// ARGV[2]: limit
	pollScript = `
		local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
		if #tasks > 0 then
			redis.call('ZREM', KEYS[1], unpack(tasks))
		end
		return tasks
	`

	// Lua: 添加任务（仅当任务不存在或新时间更早时更新）
	addIfEarlierScript = `
		local current = redis.call('ZSCORE', KEYS[1], ARGV[2])
		if current == false or tonumber(ARGV[1]) < tonumber(current) then
			redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
			return 1
		end
		return 0
	`
)

type redisDelay struct {
	client *redis.Client
	key    string
}

func NewRedisDelay(client *redis.Client, key string) Queue {
	if key == "" {
		key = "delay_queue"
	}
	return &redisDelay{
		client: client,
		key:    key,
	}
}

func (d *redisDelay) Add(ctx context.Context, taskID string, executeAt time.Time) error {
	return d.AddWithScore(ctx, taskID, executeAt.UnixMilli())
}

func (d *redisDelay) AddWithScore(ctx context.Context, taskID string, score int64) error {
	return d.client.ZAdd(ctx, d.key, redis.Z{
		Score:  float64(score),
		Member: taskID,
	}).Err()
}

// AddIfEarlier 仅当新时间更早时才更新（防止覆盖更早的调度）
func (d *redisDelay) AddIfEarlier(ctx context.Context, taskID string, executeAt time.Time) (bool, error) {
	result, err := d.client.Eval(ctx, addIfEarlierScript, []string{d.key}, executeAt.UnixMilli(), taskID).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (d *redisDelay) Poll(ctx context.Context, limit int64) ([]string, error) {
	now := time.Now().UnixMilli()

	result, err := d.client.Eval(ctx, pollScript, []string{d.key}, now, limit).Result()
	if err != nil {
		return nil, err
	}

	// 解析结果
	items, ok := result.([]interface{})
	if !ok {
		return nil, nil
	}

	tasks := make([]string, 0, len(items))
	for _, item := range items {
		if taskID, ok := item.(string); ok {
			tasks = append(tasks, taskID)
		}
	}

	return tasks, nil
}

func (d *redisDelay) Remove(ctx context.Context, taskID string) error {
	return d.client.ZRem(ctx, d.key, taskID).Err()
}

func (d *redisDelay) Len(ctx context.Context) (int64, error) {
	return d.client.ZCard(ctx, d.key).Result()
}

// PeekExpired 查看到期任务但不删除（用于调试或监控）
func (d *redisDelay) PeekExpired(ctx context.Context, limit int64) ([]string, error) {
	now := time.Now().UnixMilli()
	return d.client.ZRangeByScore(ctx, d.key, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   formatScore(now),
		Count: limit,
	}).Result()
}

// PeekByRange 查看指定时间范围的任务
func (d *redisDelay) PeekByRange(ctx context.Context, start, end time.Time, limit int64) ([]string, error) {
	return d.client.ZRangeByScore(ctx, d.key, &redis.ZRangeBy{
		Min:   formatScore(start.UnixMilli()),
		Max:   formatScore(end.UnixMilli()),
		Count: limit,
	}).Result()
}

func formatScore(score int64) string {
	return strconv.FormatInt(score, 10)
}
