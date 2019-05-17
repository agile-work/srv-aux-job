package controllers

import (
	"fmt"
	"strings"
	"sync"
	"time"

	shared "github.com/agile-work/srv-shared"
	"github.com/agile-work/srv-shared/amqp"
	"github.com/agile-work/srv-shared/sql-builder/builder"
	"github.com/agile-work/srv-shared/sql-builder/db"
)

// Job represents an running instance of the job definition
type Job struct {
	ID           string    `json:"id" sql:"id"`
	ServiceID    string    `json:"service_id" sql:"service_id"`
	Status       string    `json:"status" sql:"status"`
	Start        time.Time `json:"start_at" sql:"start_at"`
	Finish       time.Time `json:"finish_at" sql:"finish_at"`
	Params       []Param   `json:"parameters" sql:"parameters" field:"jsonb"`
	Tasks        []Task    `json:"tasks"`
	SystemParams map[string]string
	Execution    chan *Task
	Responses    chan *Task
	Instance     int
	Concurrency  int
	WG           sync.WaitGroup
}

func (j *Job) run(serviceID string) {
	db.LoadStruct(shared.TableCoreJobInstances, j, builder.Equal("id", j.ID))
	db.LoadStruct(shared.TableCoreJobTaskInstances, &j.Tasks, builder.Equal("job_instance_id", j.ID))
	//TODO: verify db loadstruct error and update job with status fail

	j.Start = time.Now()
	j.Status = shared.StatusProcessing
	j.ServiceID = serviceID

	//db.UpdateStruct(shared.TableCoreJobInstances, j, builder.Equal("id", j.ID), "start_at", "status", "service_id")
	fmt.Printf("Service ID: %s | Worker: %02d | JOB Instance ID: %s | Total tasks: %d\n", j.ServiceID, j.Instance, j.ID, len(j.Tasks))

	j.WG.Add(len(j.Tasks))
	j.defineTasksToExecute("", "", 0)
	j.WG.Wait()

	j.Finish = time.Now()
	//TODO check if there were any errors before defining status completed
	j.Status = shared.StatusCompleted
	//db.UpdateStruct(shared.TableCoreJobInstances, j, builder.Equal("id", j.ID), "finish_at", "status")

	duration := time.Since(j.Start)
	fmt.Printf("Service ID: %s | Worker: %02d | Completed in %fs\n", j.ServiceID, j.Instance, duration.Seconds())
}

func (j *Job) work() {
	for tsk := range j.Execution {
		j.parseTaskParams(tsk)
		tsk.Run(j.Responses)
	}
}

func (j *Job) response() {
	for tsk := range j.Responses {
		if tsk.Status == shared.StatusFail {
			j.Status = shared.StatusFail
		}
		j.WG.Done()
		j.defineTasksToExecute(tsk.ID, tsk.ParentID, tsk.Sequence)
	}
}

// Process keep checkin channel to process job messages
func (j *Job) Process(jobs <-chan *amqp.Message, serviceID string) {

	for i := 0; i < j.Concurrency; i++ {
		go j.work()
	}

	go func() {
		for tsk := range j.Responses {
			j.WG.Done()
			j.defineTasksToExecute(tsk.TaskID, tsk.ParentID, tsk.Sequence)
		}
	}()

	fmt.Printf("Worker %02d started [Tasks: %02d]\n", j.Instance, j.Concurrency)
	for msg := range jobs {
		j.ID = msg.ID
		j.run(serviceID)
	}
}

func (j *Job) defineTasksToExecute(id, parentID string, sequence int) {
	//check if sequence is completed
	sequenceCompleted := true
	for _, t := range j.Tasks {
		if t.ParentID == parentID && t.Sequence == sequence && (t.Status == shared.StatusProcessing || t.Status == shared.StatusCreated) {
			sequenceCompleted = false
		}
	}

	if sequenceCompleted {
		sequence++
	}

	for i, t := range j.Tasks {
		if t.ParentID == parentID && t.Sequence == sequence && t.Status == shared.StatusCreated {
			j.Tasks[i].Status = shared.StatusProcessing
			j.Execution <- &j.Tasks[i]
		}
	}

	if id != "" {
		//Check if has childs to start executing
		for i, t := range j.Tasks {
			if t.ParentID == id && t.Sequence == 0 && t.Status == shared.StatusCreated {
				j.Tasks[i].Status = shared.StatusProcessing
				j.Execution <- &j.Tasks[i]
			}
		}
	}
}

func (j *Job) getParamValue(path string) string {
	param := strings.Split(path[1:len(path)-1], ".")

	switch strings.ToLower(param[0]) {
	case paramScopeSystem:
		return j.SystemParams[param[1]]
	case paramScopeJob:
		for _, p := range j.Params {
			if param[1] == p.Key {
				return p.String()
			}
		}
	case paramScopeTask:
		for _, t := range j.Tasks {
			if t.Code == param[1] {
				return t.getParamValue(param[2])
			}
		}
	default:
		return ""
	}
	return ""
}

func (j *Job) parseTaskParams(tsk *Task) {
	refParams := tsk.getReferenceParams()
	for _, param := range refParams {
		value := j.getParamValue(param)
		tsk.ExecAddress = strings.ReplaceAll(tsk.ExecAddress, param, value)
		tsk.ExecPayload = strings.ReplaceAll(tsk.ExecPayload, param, value)
		//TODO: check rollback address and payload for params
	}
}
