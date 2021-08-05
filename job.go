package job

import (
	"encoding/gob"
	"reflect"
	"runtime"
	"sync"
	"time"
)

type (
	JobFunc func(*Job) error
)

type Job struct {
	Name    string
	StartAt time.Time
	Retry   int
	Vals    map[string]interface{}
	Fn      JobFunc
	FnName  string
}

func (j *Job) Get(key string) (val interface{}, ok bool) {
	val, ok = j.Vals[key]
	return val, ok
}

func (j *Job) GetString(key string) string {
	val, _ := j.Vals[key].(string)
	return val
}

func (j *Job) GetBytes(key string) []byte {
	val, _ := j.Vals[key].([]byte)
	return val
}

func (j *Job) GetInt(key string) int {
	val, _ := j.Vals[key].(int)
	return val
}

func (j *Job) GetBool(key string) bool {
	val, _ := j.Vals[key].(bool)
	return val
}

func (j *Job) GetFloat(key string) float64 {
	val, _ := j.Vals[key].(float64)
	return val
}

func (j *Job) GetDuration(key string) time.Duration {
	val, _ := j.Vals[key].(time.Duration)
	return val
}

func (j *Job) GetTime(key string) time.Time {
	val, _ := j.Vals[key].(time.Time)
	return val
}

func init() {
	gob.Register(&Job{})
}

var jobsHandler sync.Map

func RegisterHandler(fn JobFunc) {
	name := handlerName(fn)

	jobsHandler.LoadOrStore(name, fn)
}

func loadHandle(name string) JobFunc {
	if val, ok := jobsHandler.Load(name); ok {
		if fn, ok := val.(JobFunc); ok {
			return fn
		}
	}

	return nil
}

func handlerName(fn JobFunc) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}
