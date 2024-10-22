package scheduler

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/preempt"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"golang.org/x/sync/semaphore"
	"log/slog"
	"time"
)

type PreemptScheduler struct {
	executionDAO      storage.ExecutionDAO
	taskCfgRepository storage.TaskCfgRepository
	executors         map[string]executor.Executor
	refreshInterval   time.Duration
	limiter           *semaphore.Weighted
	logger            *slog.Logger
	pe                preempt.Preempter
}

func NewPreemptScheduler(executionDAO storage.ExecutionDAO,
	refreshInterval time.Duration, limiter *semaphore.Weighted, logger *slog.Logger,
	preempter preempt.Preempter, taskCfgRepository storage.TaskCfgRepository) *PreemptScheduler {
	return &PreemptScheduler{
		executionDAO:      executionDAO,
		refreshInterval:   refreshInterval,
		limiter:           limiter,
		executors:         make(map[string]executor.Executor),
		logger:            logger,
		pe:                preempter,
		taskCfgRepository: taskCfgRepository,
	}
}

func (p *PreemptScheduler) RegisterExecutor(execs ...executor.Executor) {
	for _, exec := range execs {
		p.executors[exec.Name()] = exec
	}
}

func (p *PreemptScheduler) Schedule(ctx context.Context) error {
	for {

		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := p.limiter.Acquire(ctx, 1)
		if err != nil {
			return err
		}

		timeout, cancel := context.WithTimeout(ctx, time.Second*3)
		leaser, err := p.pe.Preempt(timeout)
		cancel()
		if err != nil {
			p.logger.Error("抢占任务失败,可能没有任务了",
				slog.Any("error", err))
			p.limiter.Release(1)
			time.Sleep(time.Second * 3)
			continue
		}

		t := leaser.GetTask()
		exec, ok := p.executors[t.Executor]
		if !ok {
			p.logger.Error("找不到任务的执行器",
				slog.Int64("TaskID", t.ID),
				slog.String("Executor", t.Executor))
			p.ReleaseTask(leaser, t)
			continue
		}

		go p.doTaskWithAutoRefresh(ctx, leaser, exec)
	}
}

func (p *PreemptScheduler) doTaskWithAutoRefresh(ctx context.Context, l preempt.TaskLeaser, exec executor.Executor) {
	t := l.GetTask()

	defer func() {
		p.ReleaseTask(l, t)
	}()

	cancelCtx, cancelCause := context.WithCancelCause(ctx)
	ch, err := l.AutoRefresh(cancelCtx)
	defer cancelCause(nil)

	if err != nil {
		cancelCause(err)
		return
	}

	go func() {
		for {
			s, ok := <-ch
			if ok && s.Err() != nil {
				cancelCause(s.Err())
				return
			}
		}
	}()

	timeout := exec.TaskTimeout(t)
	execCtx, execCancel := context.WithTimeout(cancelCtx, timeout)
	defer execCancel()

	needRun := p.exploreLastExecution(execCtx, t, exec)
	if needRun {
		p.doTask(execCtx, t, exec)
	}

}

func (p *PreemptScheduler) exploreLastExecution(ctx context.Context, t task.Task, exec executor.Executor) bool {
	if t.LastStatus != task.TaskStatusRunning {
		return true
	}

	lastExecution, err := p.executionDAO.GetLastExecution(ctx, t.ID)
	if err != nil {
		return true
	}
	if lastExecution.Status != task.ExecStatusRunning && lastExecution.Status != task.ExecStatusUnknown {
		return true
	}
	eid := lastExecution.ID
	status, progress, err := p.exploreOnce(ctx, t, exec, eid)
	if err != nil {
		p.logger.Error("探查上次执行结果失败", slog.Int64("task_id", t.ID), slog.Int64("execution_id", eid), slog.Any("err", err))
		_ = p.updateProgressStatus(eid, 0, task.ExecStatusUnknown)
		return true
	}
	if status != task.ExecStatusRunning {
		_ = p.updateProgressStatus(eid, progress, status)
		return true
	}

	//  调整 执行超时时间 = 任务记录 创建时间 + 最大执行时间
	expectStopTime := lastExecution.Ctime.Add(exec.TaskTimeout(t))
	nctx, cancel := context.WithDeadline(ctx, expectStopTime)
	defer cancel()
	p.explore(nctx, exec, t, eid)
	return false
}

func (p *PreemptScheduler) ReleaseTask(l preempt.TaskLeaser, t task.Task) {
	p.limiter.Release(1)
	nctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	err := l.Release(nctx)
	if err != nil {
		p.logger.Error("任务释放失败", slog.Int64("task_id", t.ID),
			slog.Any("err", err))
	}
}

func (p *PreemptScheduler) doTask(ctx context.Context, t task.Task, exec executor.Executor) {
	eid, err := p.executionDAO.Create(ctx, t.ID)
	if err != nil {
		return
	}
	status, err := exec.Run(ctx, t, eid)
	progress := 0
	if status == task.ExecStatusSuccess {
		progress = 100
	}
	_ = p.updateProgressStatus(eid, progress, status)
	if err != nil || status != task.ExecStatusRunning {
		return
	}
	p.explore(ctx, exec, t, eid)

}

func (p *PreemptScheduler) exploreOnce(ctx context.Context, t task.Task, exec executor.Executor, eid int64) (task.ExecStatus, int, error) {
	nctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := exec.Explore(nctx, eid, t)
	if ch == nil {
		return task.ExecStatusUnknown, 0, errs.ErrTaskNotSupportExplore
	}

	select {
	case <-ctx.Done():
		return task.ExecStatusUnknown, 0, ctx.Err()
	case res := <-ch:
		return p.from(res.Status), res.Progress, nil
	}

}

func (p *PreemptScheduler) explore(ctx context.Context, exec executor.Executor, t task.Task, eid int64) {

	ch := exec.Explore(ctx, eid, t)
	if ch == nil {
		return
	}
	// 保存每一次探查时的进度，确保执行ctx.Done()分支时进度不会更新为零值
	progress := 0
	status := task.ExecStatusUnknown
	for {
		select {
		case <-ctx.Done():
			// 主动取消或者超时
			err := ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				status = task.ExecStatusDeadlineExceeded
			} else {
				status = task.ExecStatusCancelled
			}
			_ = p.stopTask(exec, t, eid)

		case res, ok := <-ch:
			if !ok {
				select {
				case <-ctx.Done():
					continue
				default:
					status = task.ExecStatusUnknown
				}
			} else {
				progress = res.Progress
				status = p.from(res.Status)
			}

		}

		_ = p.updateProgressStatus(eid, progress, status)
		if status != task.ExecStatusRunning {
			return
		}
	}
}

func (p *PreemptScheduler) from(status executor.Status) task.ExecStatus {
	switch status {
	case executor.StatusSuccess:
		return task.ExecStatusSuccess
	case executor.StatusFailed:
		return task.ExecStatusFailed
	default:
		return task.ExecStatusRunning
	}
}

func (p *PreemptScheduler) updateProgressStatus(eid int64, progress int, status task.ExecStatus) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	err := p.executionDAO.Update(ctx, eid, status, progress)
	if err != nil {
		p.logger.Error("更新任务记录失败", slog.Int64("execution_id", eid),
			slog.String("exec_status", status.String()), slog.Int("progress", progress),
			slog.Any("error", err))
	}
	return err
}

func (p *PreemptScheduler) stopTask(exec executor.Executor, t task.Task, eid int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	err := exec.Stop(ctx, t, eid)
	if err != nil {
		p.logger.Error("关停任务失败", slog.Int64("task_id", t.ID), slog.Int64("execution_id", eid),
			slog.Any("error", err))
	}
	return err
}
