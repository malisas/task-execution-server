package tesTaskEngineWorker

import (
	context "golang.org/x/net/context"
	"log"
	"os"
	"sync/atomic"
	"tes/ga4gh"
	"tes/server/proto"
	"time"
	//proto "github.com/golang/protobuf/proto"
)

// ForkManager documentation
// TODO: documentation
type ForkManager struct {
	procCount int
	running   bool
	files     FileMapper
	sched     ga4gh_task_ref.SchedulerClient
	workerID  string
	ctx       context.Context
	checkFunc func(status EngineStatus)
	status    EngineStatus
}

func (forkManager *ForkManager) worker(inchan chan ga4gh_task_exec.Job) {
	for job := range inchan {
		atomic.AddInt32(&forkManager.status.ActiveJobs, 1)
		atomic.AddInt32(&forkManager.status.JobCount, 1)
		log.Printf("Launch job: %s", job)
		s := ga4gh_task_exec.State_Running
		// What is UpdateJobStatus doing? Doesn't it return a JobId and a possible error? Why doesnt it get stored, and what is the internal Invoke() function doing?
		forkManager.sched.UpdateJobStatus(forkManager.ctx, &ga4gh_task_ref.UpdateStatusRequest{Id: job.JobId, State: s})
		err := RunJob(&job, forkManager.files)
		if err != nil {
			log.Printf("Job error: %s", err)
			forkManager.sched.UpdateJobStatus(forkManager.ctx, &ga4gh_task_ref.UpdateStatusRequest{Id: job.JobId, State: ga4gh_task_exec.State_Error})
		} else {
			forkManager.sched.UpdateJobStatus(forkManager.ctx, &ga4gh_task_ref.UpdateStatusRequest{Id: job.JobId, State: ga4gh_task_exec.State_Complete})
		}
		atomic.AddInt32(&forkManager.status.ActiveJobs, -1)
	}
}

// TODO: sched is the same things as filestore.sched
func (forkManager *ForkManager) watcher(sched ga4gh_task_ref.SchedulerClient, filestore FileMapper) {
	forkManager.sched = sched
	forkManager.files = filestore
	hostname, _ := os.Hostname()
	// Make a channel which can take up to 10 jobs before blocking.
	jobchan := make(chan ga4gh_task_exec.Job, 10)
	// For each nworker (forkManager.procCount), call forkManager.worker
	for i := 0; i < forkManager.procCount; i++ {
		go forkManager.worker(jobchan)
	}
	var sleepSize int64 = 1
	// forkManager.running is never modified, and this is always
	// true. watcher will enter an infinite loop here. We probably
	// want a better exiting strategy
	for forkManager.running {
		if forkManager.checkFunc != nil {
			forkManager.checkFunc(forkManager.status)
		}
		// Get a JobResponse from passing worker info to GetJobToRun
		task, err := forkManager.sched.GetJobToRun(forkManager.ctx,
			&ga4gh_task_ref.JobRequest{
				Worker: &ga4gh_task_ref.WorkerInfo{
					Id:       forkManager.workerID,
					Hostname: hostname,
					LastPing: time.Now().Unix(),
				},
			})
		if err != nil {
			log.Print(err)
		}
		if task != nil && task.Job != nil {
			sleepSize = 1
			log.Printf("Found job: %s", task)
			jobchan <- *task.Job
		} else {
			//log.Printf("No jobs found")
			if sleepSize < 20 {
				//  sleepSize += 1
			}
			time.Sleep(time.Second * time.Duration(sleepSize))
		}
	}
	close(jobchan)
}

// Start documentation
// TODO: documentation
// TODO: Run, Start, and watcher all do the same thing. Simplify?
func (forkManager *ForkManager) Start(engine ga4gh_task_ref.SchedulerClient, files FileMapper) {
	go forkManager.watcher(engine, files)
}

// Run documentation
// TODO: documentation
func (forkManager *ForkManager) Run(engine ga4gh_task_ref.SchedulerClient, files FileMapper) {
	forkManager.watcher(engine, files)
}

// SetStatusCheck documentation
// TODO: documentation
func (forkManager *ForkManager) SetStatusCheck(checkFunc func(status EngineStatus)) {
	forkManager.checkFunc = checkFunc
}

// NewLocalManager documentation
// TODO: documentation
func NewLocalManager(procCount int, workerID string) (*ForkManager, error) {
	return &ForkManager{
		procCount: procCount,
		running:   true,
		workerID:  workerID,
		ctx:       context.Background(),
	}, nil
}
