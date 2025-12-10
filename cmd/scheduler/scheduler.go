package scheduler

import (
	"fmt"
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/histephenwang/task-scheduler/internal/config"
	"github.com/histephenwang/task-scheduler/internal/infra/db/mysql"
	infraRedis "github.com/histephenwang/task-scheduler/internal/infra/redis"
	"github.com/histephenwang/task-scheduler/internal/lock"
	"github.com/histephenwang/task-scheduler/internal/timewheel"
)

func main() {
	cfg, err := config.Load("configs/dev.toml")
	if err != nil {
		panic(err)
	}

	db := mysql.NewDB(&cfg.Database)
	rc := infraRedis.NewClient(&cfg.Redis)

	defer rc.Close()

	// 初始化时间轮： 假设 tick=100ms，200个槽，那么最大支持 20秒
	if cfg.Timing.Tick == 0 {
		cfg.Timing.Tick = 500 * time.Millisecond
	}
	if cfg.Timing.Size == 0 {
		cfg.Timing.Size = 10
	}

	tw := timingwheel.NewTimingWheel(cfg.Timing.Tick, cfg.Timing.Size)
	tw.Start()

	defer tw.Stop()

	distLock := lock.NewRedisLock(rc, "scheduler-lock")
	delayLock := lock.NewRedisLock(rc, "scheduler-delay")

	timewheel.New(cfg.Timing.Tick, cfg.Timing.Size, func(taskID string) {

	})

	fmt.Println(db, distLock, delayLock)
}
