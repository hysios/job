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

func (j *Job) GetString(key string) (val string, ok bool) {
	val, ok = j.Vals[key].(string)
	return val, ok
}

func (j *Job) GetInt(key string) (val int, ok bool) {
	val, ok = j.Vals[key].(int)
	return val, ok
}

func (j *Job) GetBool(key string) (val bool, ok bool) {
	val, ok = j.Vals[key].(bool)
	return val, ok
}

func (j *Job) GetFloat(key string) (val float64, ok bool) {
	val, ok = j.Vals[key].(float64)
	return val, ok
}

func (j *Job) GetDuration(key string) (val time.Duration, ok bool) {
	val, ok = j.Vals[key].(time.Duration)
	return val, ok
}

func (j *Job) GetTime(key string) (val time.Time, ok bool) {
	val, ok = j.Vals[key].(time.Time)
	return val, ok
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
