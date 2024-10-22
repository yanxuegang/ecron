package mysql

import (
	"context"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

type GormExecutionDAO struct {
	db *gorm.DB
}

func (h *GormExecutionDAO) Update(ctx context.Context, eid int64, status task.ExecStatus, progress int) error {
	return h.db.WithContext(ctx).Model(&Execution{}).
		Where("id = ?", eid).Updates(map[string]any{
		"progress": uint8(progress),
		"status":   status.ToUint8(),
		"utime":    time.Now().UnixMilli(),
	}).Error
}

func (h *GormExecutionDAO) ToDomain(e Execution) task.Execution {
	return task.Execution{
		ID:       e.ID,
		Tid:      e.Tid,
		Status:   task.ExecStatus(e.Status),
		Progress: e.Progress,
		Ctime:    time.UnixMilli(e.Ctime),
		Utime:    time.UnixMilli(e.Utime),
	}
}

func (h *GormExecutionDAO) Create(ctx context.Context, tid int64) (int64, error) {
	now := time.Now().UnixMilli()
	exec := Execution{
		Tid:      tid,
		Status:   uint8(task.ExecStatusRunning),
		Progress: 0,
		Ctime:    now,
		Utime:    now,
	}
	err := h.db.WithContext(ctx).Create(&exec).Error
	return exec.ID, err
}

func (h *GormExecutionDAO) GetLastExecution(ctx context.Context, tid int64) (task.Execution, error) {
	var exec Execution
	err := h.db.WithContext(ctx).Where("tid = ?", tid).Last(&exec).Error
	return h.ToDomain(exec), err
}

func NewGormExecutionDAO(db *gorm.DB) storage.ExecutionDAO {
	return &GormExecutionDAO{db: db}
}

func (h *GormExecutionDAO) Upsert(ctx context.Context, id int64, status task.ExecStatus, progress uint8) (int64, error) {
	now := time.Now().UnixMilli()
	exec := Execution{
		Tid:      id,
		Status:   status.ToUint8(),
		Progress: progress,
		Ctime:    now,
		Utime:    now,
	}
	err := h.db.WithContext(ctx).Clauses(clause.OnConflict{
		DoUpdates: clause.Assignments(map[string]any{
			"status":   status.ToUint8(),
			"progress": progress,
			"utime":    now,
		}),
	}).Create(&exec).Error
	return exec.ID, err
}
