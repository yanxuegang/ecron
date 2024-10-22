package executor

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/task"
	"log/slog"
	"time"
)

var _ Executor = (*LocalExecutor)(nil)

type LocalExecutor struct {
	logger *slog.Logger
	fn     map[string]func(ctx context.Context, t task.Task) error
}

func (l *LocalExecutor) Stop(ctx context.Context, t task.Task, eid int64) error {
	return nil
}

func NewLocalExecutor(logger *slog.Logger) *LocalExecutor {
	return &LocalExecutor{
		logger: logger,
		fn:     make(map[string]func(ctx context.Context, t task.Task) error),
	}
}

func (l *LocalExecutor) RegisterFunc(name string, fn func(ctx context.Context, t task.Task) error) {
	l.fn[name] = fn
}

func (l *LocalExecutor) Name() string {
	return "LOCAL"
}

func (l *LocalExecutor) Run(ctx context.Context, t task.Task, eid int64) (task.ExecStatus, error) {
	fn, ok := l.fn[t.Name]
	if !ok {
		l.logger.Error("未知执行方法的任务",
			slog.Int64("ID", t.ID),
			slog.String("Name", t.Name))
		return task.ExecStatusFailed, errs.ErrUnknownTask
	}
	err := fn(ctx, t)
	switch {
	case err == nil:
		return task.ExecStatusSuccess, nil
	case errors.Is(err, context.Canceled):
		return task.ExecStatusCancelled, nil
	case errors.Is(err, context.DeadlineExceeded):
		return task.ExecStatusDeadlineExceeded, nil
	default:
		return task.ExecStatusFailed, err
	}
}

func (l *LocalExecutor) Explore(ctx context.Context, eid int64, t task.Task) <-chan Result {
	// 在我们的默认实现中，本地任务不支持任务探查，需要的用户可以自己实现
	return nil
}

func (l *LocalExecutor) TaskTimeout(t task.Task) time.Duration {
	var result LocalCfg
	err := json.Unmarshal([]byte(t.Cfg), &result)
	if err != nil || result.TaskTimeout < 0 {
		return time.Minute
	}
	return result.TaskTimeout
}

type LocalCfg struct {
	TaskTimeout time.Duration `json:"taskTimeout"`
	// 任务探查间隔
	ExploreInterval time.Duration `json:"exploreInterval"`
}
