package job

import (
	"bytes"
	"encoding/gob"
	"sync"
	"time"

	"github.com/hysios/log"
	"github.com/tidwall/buntdb"
)

const (
	DefaultInterval = 10 * time.Second
	MaxRetryTime    = 5 * time.Minute
)

type Manager struct {
	Interval        time.Duration
	MaxRetryTimeout time.Duration
	db              *buntdb.DB
	jobs            sync.Map
	job             chan *Job
}

type ManagerOption struct {
	DBName string
}

func NewManager(opts ...OptFunc) *Manager {
	var (
		manager = &Manager{
			Interval:        DefaultInterval,
			MaxRetryTimeout: MaxRetryTime,
			job:             make(chan *Job),
		}
		opt ManagerOption
	)

	for _, op := range opts {
		op(&opt)
	}

	manager.initDB(opt.DBName)
	go manager.runJobs()

	return manager
}

func (manager *Manager) initDB(dbfile string) error {
	db, err := buntdb.Open(dbfile)
	if err != nil {
		return err
	}

	manager.db = db
	return nil
}

func (manager *Manager) runJobs() {
	manager.LoadJobs()

	if manager.Interval == 0 {
		manager.Interval = DefaultInterval
	}

	if manager.MaxRetryTimeout == 0 {
		manager.MaxRetryTimeout = MaxRetryTime
	}

	for {
		select {
		case job := <-manager.job:
			if err := job.Fn(job); err != nil {
				job.Retry++
				log.Infof("run job error %s retry %d", err, job.Retry)
				manager.SaveJob(job)
			} else {
				manager.ClearJob(job)
			}
		case t := <-time.After(manager.Interval):
			log.Infof("run check %s", t)
			manager.jobs.Range(func(key interface{}, val interface{}) bool {
				if job, ok := val.(*Job); ok {
					if job.StartAt.Add(manager.MaxRetryTimeout).Before(t) {
						log.Infof("out of max timeout %s, clear job", manager.MaxRetryTimeout)
						manager.ClearJob(job)
						return true
					}

					if job.Fn == nil {
						log.Info("job callback is empty")
						manager.ClearJob(job)
						return true
					}

					if err := job.Fn(job); err != nil {
						job.Retry++
						log.Infof("run job error %s retry %d", err, job.Retry)
						manager.SaveJob(job)
					} else {
						manager.ClearJob(job)
					}
				}
				return true
			})
		}
	}
}

func (manager *Manager) AddJob(name string, inital map[string]interface{}, fn JobFunc) {
	if inital == nil {
		inital = make(map[string]interface{})
	}

	RegisterHandler(fn)
	log.Infof("add job %s", name)

	var job = &Job{
		Name:    name,
		StartAt: time.Now(),
		Retry:   0,
		Vals:    inital,
		Fn:      fn,
	}

	manager.jobs.LoadOrStore(name, job)
	if err := manager.SaveJob(job); err != nil {
		log.Infof("save error %s", err)
	}

	manager.job <- job
}

func (manager *Manager) LoadJobs() error {
	return manager.db.View(func(tx *buntdb.Tx) error {
		tx.Ascend("", func(key, value string) bool {
			log.Infof("load job: %s with %d bytes\n", key, len(value))
			job, err := manager.LoadJob([]byte(value))
			if err != nil {
				log.Infof("load job %s error %s", key, err)
				return true
			}
			manager.jobs.Store(job.Name, job)
			return true
		})
		return nil
	})
}

func (manager *Manager) LoadJob(b []byte) (*Job, error) {
	var (
		dec = gob.NewDecoder(bytes.NewBuffer(b))
		job Job
		err error
	)
	if err = dec.Decode(&job); err != nil {
		return nil, err
	}
	job.Fn = loadHandle(job.FnName)

	log.Infof("load job %s\n % #v", job.Name, job)
	return &job, nil
}

func (manager *Manager) SaveJob(job *Job) error {
	return manager.db.Update(func(tx *buntdb.Tx) error {
		var (
			buf bytes.Buffer
			enc = gob.NewEncoder(&buf)
			err error
		)
		job.FnName = handlerName(job.Fn)
		if err = enc.Encode(job); err != nil {
			return err
		}
		log.Infof("save job %s with %d bytes\n", job.Name, buf.Len())
		tx.Set(job.Name, string(buf.Bytes()), nil)
		return nil
	})
}

func (manager *Manager) ClearJob(job *Job) error {
	return manager.db.Update(func(tx *buntdb.Tx) error {
		log.Infof("clear job %s\n", job.Name)
		tx.Delete(job.Name)
		manager.jobs.Delete(job.Name)
		return nil
	})
}

var manager *Manager

func ManagerStart(opts ...OptFunc) error {
	manager = NewManager(opts...)
	return nil
}

func AddJob(name string, inital map[string]interface{}, fn JobFunc) {
	manager.AddJob(name, inital, fn)
}

func ClearJob(job *Job) {
	manager.ClearJob(job)
}

func LoadJob(b []byte) (*Job, error) {
	return manager.LoadJob(b)
}
