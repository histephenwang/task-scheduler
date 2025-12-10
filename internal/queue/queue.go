package queue

import (
	"context"

	"github.com/histephenwang/task-scheduler/internal/model"
)

type TaskQueue interface {
	Push(ctx context.Context, task *model.Task) error
	Consume(ctx context.Context, handler string) error
	Close() error
}
