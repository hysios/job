package job

import (
	"testing"
)

func TestNewManager(t *testing.T) {
	var manager = NewManager(DBName("test.db"))

	manager.AddJob("testjob", nil, func(j *Job) error {
		t.Run("callback", func(tt *testing.T) {
			tt.Logf("test")
		})
		return nil
	})
}
