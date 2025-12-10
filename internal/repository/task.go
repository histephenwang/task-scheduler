package repository

import (
	"context"
	"errors"
	"time"

	"github.com/histephenwang/task-scheduler/internal/model"
	"github.com/jmoiron/sqlx"
)

type TaskRepository interface {
	LoadPendingDelay(ctx context.Context, before time.Time, limit int) ([]*model.Task, error)
	LoadCronTask(ctx context.Context) ([]*model.Task, error)
	GetByID(ctx context.Context, id string) (*model.Task, error)
	ListPendingDelayed(ctx context.Context, end time.Time, limit int) ([]*model.Task, error)
	UpdateStatus(ctx context.Context, taskID string, from, to string) error
	Create(ctx context.Context, task *model.Task) error
	Update(ctx context.Context, task *model.Task) error

	UpdateStatusWithLock(ctx context.Context, taskID string, from, to int) error
}

type taskRepository struct {
	db *sqlx.DB
}

func NewTaskRepository(db *sqlx.DB) TaskRepository {
	return &taskRepository{
		db: db,
	}
}

func (r *taskRepository) Update(ctx context.Context, task *model.Task) error {
	panic("implement me")
}

func (r *taskRepository) Create(ctx context.Context, task *model.Task) error {
	panic("implement me")
}

func (r *taskRepository) UpdateStatus(ctx context.Context, taskID string, from, to string) error {
	panic("implement me")
}

func (r *taskRepository) ListPendingDelayed(ctx context.Context, end time.Time, limit int) ([]*model.Task, error) {
	panic("implement me")
}

func (r *taskRepository) GetByID(ctx context.Context, id string) (*model.Task, error) {
	panic("implement me")
}

func (r *taskRepository) LoadPendingDelay(ctx context.Context, before time.Time, limit int) ([]*model.Task, error) {
	const query = `
		SELECT * FROM task 
			WHERE status = ?
			ADN type = ?
			AND created_at < ?
		ORDER BY created_at DESC LIMIT ?
	`

	args := []interface{}{model.Pending, model.Delay, before, limit}
	var tasks []*model.Task
	err := r.db.SelectContext(ctx, &tasks, query, args...)
	if err != nil {
		return nil, errors.New("failed to load pending task")
	}

	return tasks, nil
}

func (r *taskRepository) LoadCronTask(ctx context.Context) ([]*model.Task, error) {
	const query = `
		SELECT * FROM task
			WHERE status = ?
			AND type = ?
		order by created_at DESC
	`
	args := []interface{}{model.Pending, model.Cron}
	var tasks []*model.Task
	err := r.db.SelectContext(ctx, &tasks, query, args...)
	if err != nil {
		return nil, errors.New("failed to load task")
	}
	return tasks, nil
}

func (r *taskRepository) UpdateStatusWithLock(ctx context.Context, taskID string, from, to int) error {
	const sql = `
		UPDATE task
			SET status = ?
			WHERE task_id = ?
		LIMIT 1
	`
	result, err := r.db.ExecContext(ctx, sql, model.Pending, model.Cron, taskID)
	if err != nil {
		return errors.New("failed to update task")
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return errors.New("record not found")
	}
	return nil
}
