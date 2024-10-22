package executor

import (
	"context"
	"github.com/ecodeclub/ecron/internal/task"
	"time"
)

//go:generate mockgen -source=./types.go -package=executormocks -destination=./mocks/executor.mock.go
type Executor interface {
	// Name 执行器的名称
	Name() string
	// Run 执行任务
	// ctx 整个调度器的上下文，当有ctx.Done信号时，就要考虑结束任务的执行。
	// eid execution id，将这个传递给任务执行方。
	// 如果实现不支持任务探查，则不应该返回 task.ExecStatusStarted 和 task.ExecStatusRunning
	Run(ctx context.Context, t task.Task, eid int64) (task.ExecStatus, error)
	// Explore 任务进度探查。
	// 返回 <-chan Result，任务探查结果会被写进该channel中，用户收到 StatusSuccess 或 StatusFailed 时，表示探查结束。
	// 可以通过 ctx 信号主动关闭任务探查进程。
	// 是否支持任务探查有实现者决定。如果实现不支持任务探查，需要确保在 Run 方法返回任务最终结果，并且返回一个 nil channel。
	// 如何处理部分失败和超时响应取决于具体实现。
	Explore(ctx context.Context, eid int64, t task.Task) <-chan Result
	// TaskTimeout 返回任务的最大执行时间。当任务执行时长超过这个时间后，调度器会立刻取消执行任务。
	// 如果任务没有配置，实现可以设置一个默认值。
	TaskTimeout(t task.Task) time.Duration

	// Stop 取消任务执行
	Stop(ctx context.Context, t task.Task, eid int64) error
}

// Result 业务方返回的结果
type Result struct {
	Eid int64 `json:"eid"`
	// 任务状态
	Status Status `json:"status"`
	// 任务执行进度
	Progress int `json:"progress"`
}

type Status string

const (
	StatusSuccess Status = "SUCCESS"
	StatusFailed  Status = "FAILED"
	StatusRunning Status = "RUNNING"
)
