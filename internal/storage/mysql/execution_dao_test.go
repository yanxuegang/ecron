package mysql

import (
	"context"
	"database/sql"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
)

func TestGormExecutionDAO_Upsert(t *testing.T) {
	testCase := []struct {
		name       string
		sqlMock    func(t *testing.T) *sql.DB
		id         int64
		taskStatus task.ExecStatus
		wantErr    error
		wantID     int64
	}{
		{
			name: "启动任务，insert一条记录",
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("INSERT INTO `execution` .* ON DUPLICATE KEY UPDATE").
					WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			id:         1,
			taskStatus: task.ExecStatusRunning,
			wantErr:    nil,
			wantID:     1,
		},
		{
			name: "任务执行成功，更新执行记录",
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("INSERT INTO `execution` .* ON DUPLICATE KEY UPDATE").
					WillReturnResult(sqlmock.NewResult(2, 1))
				return mockDB
			},
			id:         1,
			taskStatus: task.ExecStatusSuccess,
			wantErr:    nil,
			wantID:     2,
		},
	}
	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB := tc.sqlMock(t)
			db, err := gorm.Open(mysql.New(mysql.Config{
				Conn:                      sqlDB,
				SkipInitializeWithVersion: true,
			}), &gorm.Config{
				DisableAutomaticPing:   true,
				SkipDefaultTransaction: true,
			})
			require.NoError(t, err)
			dao := NewGormExecutionDAO(db)
			id, err := dao.Upsert(context.Background(), tc.id, tc.taskStatus, 0)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantID, id)
		})
	}
}
