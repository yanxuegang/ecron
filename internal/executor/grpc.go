package executor

import (
	"context"
	"github.com/ecodeclub/ecron/internal/task"
	"time"
)

var _ Executor = (*GrpcExecutor)(nil)

type GrpcExecutor struct {
}

func NewGrpcExecutor() Executor {
	return &GrpcExecutor{}
}

func (g *GrpcExecutor) Name() string {
	return "GRPC"
}

func (g *GrpcExecutor) Run(ctx context.Context, t task.Task, eid int64) (task.ExecStatus, error) {
	//TODO implement me
	panic("implement me")
}

func (g *GrpcExecutor) Explore(ctx context.Context, eid int64, t task.Task) <-chan Result {
	//TODO implement me
	panic("implement me")
}

func (g *GrpcExecutor) TaskTimeout(t task.Task) time.Duration {
	//TODO implement me
	panic("implement me")
}

func (g *GrpcExecutor) Stop(ctx context.Context, t task.Task, eid int64) error {
	//TODO implement me
	panic("implement me")
}

type GrpcCfg struct {
	ServiceName string `json:"service_name"`
	Method      string `json:"method"`
	Port        int    `json:"port"`
}
