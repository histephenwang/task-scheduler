package mysql

import (
	"context"
	"fmt"
	"time"

	"github.com/histephenwang/task-scheduler/internal/config"
	"github.com/jmoiron/sqlx"
)

func NewDB(cfg *config.DatabaseConfig) *sqlx.DB {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)

	fmt.Println(dsn)

	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		panic(fmt.Errorf("mysql ping failed: %s", err))
	}

	return db
}
