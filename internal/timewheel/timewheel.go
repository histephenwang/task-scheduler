package timewheel

import (
	"sync"
	"time"

	"github.com/RussellLuo/timingwheel"
)

// Callback 触发回调
type Callback func(taskID string)

// TimeWheel 时间轮封装
type TimeWheel struct {
	tw       *timingwheel.TimingWheel
	timers   map[string]*timingwheel.Timer // taskID -> Timer
	callback Callback
	mu       sync.RWMutex
}

// New 创建时间轮
func New(tick time.Duration, wheelSize int64, callback Callback) *TimeWheel {
	if tick <= 0 {
		tick = 100 * time.Millisecond
	}
	if wheelSize <= 0 {
		wheelSize = 3600
	}

	return &TimeWheel{
		tw:       timingwheel.NewTimingWheel(tick, wheelSize),
		timers:   make(map[string]*timingwheel.Timer),
		callback: callback,
	}
}

// Start 启动
func (t *TimeWheel) Start() {
	t.tw.Start()
}

// Stop 停止
func (t *TimeWheel) Stop() {
	t.tw.Stop()
}

// Add 添加任务
func (t *TimeWheel) Add(taskID string, delay time.Duration) {
	if delay <= 0 {
		delay = time.Millisecond
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// 如果已存在，先移除
	if timer, exists := t.timers[taskID]; exists {
		timer.Stop()
		delete(t.timers, taskID)
	}

	// 添加新定时器
	timer := t.tw.AfterFunc(delay, func() {
		t.onTrigger(taskID)
	})

	t.timers[taskID] = timer
}

// AddAt 指定时间执行
func (t *TimeWheel) AddAt(taskID string, executeAt time.Time) {
	delay := time.Until(executeAt)
	t.Add(taskID, delay)
}

// Remove 移除任务
func (t *TimeWheel) Remove(taskID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if timer, exists := t.timers[taskID]; exists {
		stopped := timer.Stop()
		delete(t.timers, taskID)
		return stopped
	}
	return false
}

// Exists 是否存在
func (t *TimeWheel) Exists(taskID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, exists := t.timers[taskID]
	return exists
}

// Len 任务数量
func (t *TimeWheel) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.timers)
}

func (t *TimeWheel) onTrigger(taskID string) {
	// 从 map 移除
	t.mu.Lock()
	delete(t.timers, taskID)
	t.mu.Unlock()

	// 执行回调
	if t.callback != nil {
		t.callback(taskID)
	}
}
