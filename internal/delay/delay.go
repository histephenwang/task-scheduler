package delay

import (
	"context"
	"time"
)

// Queue 延迟队列接口
type Queue interface {
	// Add 添加延迟任务
	Add(ctx context.Context, taskID string, executeAt time.Time) error

	// AddWithScore 添加延迟任务（直接用时间戳）
	AddWithScore(ctx context.Context, taskID string, score int64) error

	// Poll 获取到期任务（原子操作：查询+删除）
	Poll(ctx context.Context, limit int64) ([]string, error)

	// Remove 移除任务
	Remove(ctx context.Context, taskID string) error

	// Len 队列长度
	Len(ctx context.Context) (int64, error)
}
