package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/histephenwang/task-scheduler/internal/delay"
	"github.com/histephenwang/task-scheduler/internal/lock"
	"github.com/histephenwang/task-scheduler/internal/model"
	"github.com/histephenwang/task-scheduler/internal/repository"
	"github.com/histephenwang/task-scheduler/internal/timewheel"
	"go.uber.org/zap"
)

// DelayedConfig 延迟调度器配置
type DelayedConfig struct {
	ScanInterval    time.Duration // Redis 扫描间隔
	ScanBatchSize   int64         // 每次扫描数量
	LockTTL         time.Duration // 任务锁过期时间
	TimeWheelTick   time.Duration // 时间轮精度
	TimeWheelSize   int64         // 时间轮槽数
	PreloadDuration time.Duration // 预加载范围
}

func DefaultDelayedConfig() *DelayedConfig {
	return &DelayedConfig{
		ScanInterval:    time.Second,
		ScanBatchSize:   100,
		LockTTL:         30 * time.Second,
		TimeWheelTick:   100 * time.Millisecond,
		TimeWheelSize:   3600,
		PreloadDuration: 5 * time.Second,
	}
}

// DelayedScheduler 延迟任务调度器
type DelayedScheduler struct {
	config     *DelayedConfig
	delayQueue delay.Queue
	timeWheel  *timewheel.TimeWheel
	taskRepo   repository.TaskRepository
	lock       lock.DistributedLock
	logger     *zap.Logger

	stopChan chan struct{}
	wg       sync.WaitGroup
	stopped  bool
	mu       sync.Mutex
}

// NewDelayedScheduler 创建延迟调度器
func NewDelayedScheduler(config *DelayedConfig, delayQueue delay.Queue, taskRepo repository.TaskRepository, distLock lock.DistributedLock, logger *zap.Logger) *DelayedScheduler {
	if config == nil {
		config = DefaultDelayedConfig()
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	s := &DelayedScheduler{
		config:     config,
		delayQueue: delayQueue,
		taskRepo:   taskRepo,
		lock:       distLock,
		logger:     logger.Named("delayed"),
		stopChan:   make(chan struct{}),
	}

	// 创建时间轮
	s.timeWheel = timewheel.New(
		config.TimeWheelTick,
		config.TimeWheelSize,
		s.onTimeWheelTrigger,
	)

	return s
}

// Start 启动
func (s *DelayedScheduler) Start(ctx context.Context) {
	s.logger.Info("delayed scheduler starting")

	s.timeWheel.Start()

	s.wg.Add(1)
	go s.scanLoop(ctx)

	s.logger.Info("delayed scheduler started")
}

// Stop 停止
func (s *DelayedScheduler) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.mu.Unlock()

	s.logger.Info("delayed scheduler stopping")

	close(s.stopChan)
	s.timeWheel.Stop()
	s.wg.Wait()

	s.logger.Info("delayed scheduler stopped")
}

// scanLoop 扫描循环
func (s *DelayedScheduler) scanLoop(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.config.ScanInterval)
	defer ticker.Stop()

	s.scan(ctx) // 启动时立即扫描

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.scan(ctx)
		}
	}
}

// scan 扫描 Redis
func (s *DelayedScheduler) scan(ctx context.Context) {
	// 1. 获取已到期任务，立即执行
	expiredTasks, err := s.delayQueue.Poll(ctx, s.config.ScanBatchSize)
	if err != nil {
		s.logger.Error("poll expired tasks failed", zap.Error(err))
	} else {
		for _, taskID := range expiredTasks {
			s.wg.Add(1)
			go func(id string) {
				defer s.wg.Done()
				s.dispatch(ctx, id)
			}(taskID)
		}
	}

	// 2. 预加载即将到期任务到时间轮
	s.preload(ctx)
}

// preload 预加载到时间轮
func (s *DelayedScheduler) preload(ctx context.Context) {
	now := time.Now()
	end := now.Add(s.config.PreloadDuration)

	tasks, err := s.peekUpcoming(ctx, now, end, s.config.ScanBatchSize)
	if err != nil {
		s.logger.Error("peek upcoming failed", zap.Error(err))
		return
	}

	for _, taskID := range tasks {
		// 已在时间轮中则跳过
		if s.timeWheel.Exists(taskID) {
			continue
		}

		task, err := s.taskRepo.GetByID(ctx, taskID)
		if err != nil || task == nil {
			continue
		}

		if task.Status != model.Pending {
			s.delayQueue.Remove(ctx, taskID)
			continue
		}

		// 加入时间轮
		execAt := time.Now().Add(time.Second * 5)
		s.timeWheel.AddAt(taskID, execAt)
	}
}

// peekUpcoming 查看即将到期任务
func (s *DelayedScheduler) peekUpcoming(ctx context.Context, start, end time.Time, limit int64) ([]string, error) {
	if peeker, ok := s.delayQueue.(interface {
		PeekByRange(ctx context.Context, start, end time.Time, limit int64) ([]string, error)
	}); ok {
		return peeker.PeekByRange(ctx, start, end, limit)
	}

	// 降级查数据库
	tasks, err := s.taskRepo.ListPendingDelayed(ctx, end, int(limit))
	if err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(tasks))
	for _, t := range tasks {
		ids = append(ids, t.TaskID)
	}
	return ids, nil
}

// onTimeWheelTrigger 时间轮触发
func (s *DelayedScheduler) onTimeWheelTrigger(taskID string) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.LockTTL)
	defer cancel()

	s.delayQueue.Remove(ctx, taskID)
	s.dispatch(ctx, taskID)
}

// dispatch 分发执行
func (s *DelayedScheduler) dispatch(ctx context.Context, taskID string) {
	logger := s.logger.With(zap.String("task_id", taskID))

	// 获取任务
	task, err := s.taskRepo.GetByID(ctx, taskID)
	if err != nil {
		logger.Error("get task failed", zap.Error(err))
		return
	}
	if task == nil {
		logger.Warn("task not found")
		return
	}
	if task.Status != model.Pending {
		return
	}

	// 抢锁
	lockKey := fmt.Sprintf("task:%s", taskID)
	acquired, err := s.lock.TryLock(ctx, lockKey, s.config.LockTTL)
	if err != nil || !acquired {
		return
	}

	// 更新状态
	err = s.taskRepo.UpdateStatus(ctx, taskID, model.Pending, model.Running)
	if err != nil {
		s.lock.Unlock(ctx, lockKey)
		return
	}

	fmt.Println(err)

	// todo 创建执行记录

	// todo 执行
}

// onComplete 执行完成
func (s *DelayedScheduler) onComplete(ctx context.Context, task *model.Task, lockKey string) {

	//todo  更新任务状态

	// todo 更新执行记录
}

// retry 重试
func (s *DelayedScheduler) retry(ctx context.Context, task *model.Task, errMsg string) {
	retryDelay := time.Second * time.Duration(1<<task.MaxRetry) // 指数退避
	if retryDelay > time.Hour {
		retryDelay = time.Hour
	}

	nextAt := time.Now().Add(retryDelay)

	task.MaxRetry++
	task.Status = model.Pending
	task.ExecuteAt = nextAt

	s.taskRepo.Update(ctx, task)
	s.delayQueue.Add(ctx, task.TaskID, nextAt)

	s.logger.Info("retry scheduled",
		zap.String("task_id", task.TaskID),
		zap.Int("retry", task.MaxRetry),
		zap.Time("next_at", nextAt),
	)
}

// AddTask 添加任务
func (s *DelayedScheduler) AddTask(ctx context.Context, task *model.Task) error {
	if err := s.delayQueue.Add(ctx, task.TaskID, task.ExecuteAt); err != nil {
		return err
	}

	if time.Until(task.ExecuteAt) <= s.config.PreloadDuration {
		s.timeWheel.AddAt(task.TaskID, task.ExecuteAt)
	}

	return nil
}

// RemoveTask 移除任务
func (s *DelayedScheduler) RemoveTask(ctx context.Context, taskID string) error {
	s.delayQueue.Remove(ctx, taskID)
	s.timeWheel.Remove(taskID)
	return nil
}
