package model

import "time"

const (
	Pending = "pending"
	Running = "running"
	Success = "success"
	Failed  = "failed"
)

const (
	Cron    = "cron"
	Delay   = "delay"
	Instant = "instant"
)

type Task struct {
	ID         string    `json:"id" db:"id"`
	Name       string    `json:"name" db:"name"`
	Type       string    `json:"type" db:"type"`
	CronExpr   *string   `json:"cron_expr,omitempty" db:"cron_expr"`
	DelaySec   *int64    `json:"delay_sec,omitempty" db:"delay_sec"`
	Target     string    `json:"target" db:"target"`
	MaxRetry   int       `json:"max_retry" db:"max_retry"`
	TimeoutSec int       `json:"timeout_sec" db:"timeout_sec"`
	Status     string    `json:"status" db:"status"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
	UpdatedAt  time.Time `json:"updated_at" db:"updated_at"`
}
