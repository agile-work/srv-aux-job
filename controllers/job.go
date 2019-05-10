package controllers

import (
	"fmt"
	"sync"
	"time"

	"github.com/agile-work/srv-shared/amqp"
)

// Job represents an running instance of the job definition
type Job struct {
	ID          string    `json:"id" sql:"id"`
	ServiceID   string    `json:"service_id" sql:"service_id"`
	Status      string    `json:"status" sql:"status"`
	Start       time.Time `json:"start_at" sql:"start_at"`
	Finish      time.Time `json:"finish_at" sql:"finish_at"`
	Tasks       []Task    `json:"tasks"`
	Execution   chan *Task
	Responses   chan *Task
	Instance    int
	Concurrency int
	WG          sync.WaitGroup
}

func (j *Job) run() {
	//TODO: Check and wait until JOB Instance is in created status
	j.Start = time.Now()
	j.Status = statusProcessing
	//TODO: Update job instance on DB (status, service_id)
	//TODO: Read tasks from database
	mockTasks(&j.Tasks)

	fmt.Printf("JOB: %s | Worker: %02d | JOB Instance ID: %s | Total tasks: %d\n", j.ServiceID, j.Instance, j.ID, len(j.Tasks))

	//TODO: Get Tasks params

	j.WG.Add(len(j.Tasks))
	j.defineTasksToExecute("", "", 0)
	j.WG.Wait()

	j.Finish = time.Now()
	duration := time.Since(j.Start)
	fmt.Printf("JOB: %s | Worker: %02d | Completed in %fs\n", j.ServiceID, j.Instance, duration.Seconds())
}

func (j *Job) work() {
	for tsk := range j.Execution {
		tsk.Run(j.Responses)
	}
}

func (j *Job) response() {
	for tsk := range j.Responses {
		fmt.Printf("    Task: %s | Status: %s\n", tsk.ID, tsk.Status)
		j.WG.Done()
		j.defineTasksToExecute(tsk.ID, tsk.ParentID, tsk.Sequence)
	}
}

// Process keep checkin channel to process job messages
func (j *Job) Process(jobs <-chan *amqp.Message) {

	for i := 0; i < j.Concurrency; i++ {
		go j.work()
	}

	go func() {
		for tsk := range j.Responses {
			j.WG.Done()
			j.defineTasksToExecute(tsk.ID, tsk.ParentID, tsk.Sequence)
		}
	}()

	fmt.Printf("Worker %02d started [Tasks: %02d]\n", j.Instance, j.Concurrency)
	for msg := range jobs {
		j.ID = msg.ID
		j.run()
	}
}

func (j *Job) defineTasksToExecute(id, parentID string, sequence int) {
	//check if sequence is completed
	sequenceCompleted := true
	for _, t := range j.Tasks {
		if t.ParentID == parentID && t.Sequence == sequence && (t.Status == statusProcessing || t.Status == statusCreated) {
			sequenceCompleted = false
		}
	}

	if sequenceCompleted {
		fmt.Printf("Sequence %d completed\n", sequence)
		sequence++
	}

	for i, t := range j.Tasks {
		if t.ParentID == parentID && t.Sequence == sequence && t.Status == statusCreated {
			fmt.Printf("Push to channel execution -> Task %s\n", t.ID)
			j.Tasks[i].Status = statusProcessing
			j.Execution <- &j.Tasks[i]
		}
	}

	if id != "" {
		//Check if has childs to start executing
		for i, t := range j.Tasks {
			if t.ParentID == id && t.Sequence == 0 && t.Status == statusCreated {
				fmt.Printf("Push to channel execution -> Task %s\n", t.ID)
				j.Tasks[i].Status = statusProcessing
				j.Execution <- &j.Tasks[i]
			}
		}
	}

}

func mockTasks(tasks *[]Task) {
	t := Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "AAA",
		ExecTimeout: 5,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "BBB",
		ExecTimeout: 4,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "BB1",
		ExecTimeout: 6,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "BB2",
		ExecTimeout: 2,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "BB3",
		ExecTimeout: 10,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "B31",
		ExecTimeout: 1,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "B32",
		ExecTimeout: 1,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "CCC",
		ExecTimeout: 1,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "DDD",
		ExecTimeout: 2,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "DD1",
		ExecTimeout: 5,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "DD2",
		ExecTimeout: 2,
	}
	*tasks = append(*tasks, t)
}

func mockTasksParents(tasks *[]Task) {
	t := Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "AAA",
		ExecTimeout: 2,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "BBB",
		ExecTimeout: 5,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "BB1",
		ParentID:    "BBB",
		ExecTimeout: 1,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "BB2",
		ParentID:    "BBB",
		ExecTimeout: 2,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    1,
		ID:          "BB3",
		ParentID:    "BBB",
		ExecTimeout: 10,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "B31",
		ParentID:    "BB3",
		ExecTimeout: 1,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    1,
		ID:          "B32",
		ParentID:    "BB3",
		ExecTimeout: 1,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    1,
		ID:          "CCC",
		ExecTimeout: 1,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    2,
		ID:          "DDD",
		ExecTimeout: 2,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    0,
		ID:          "DD1",
		ParentID:    "DDD",
		ExecTimeout: 5,
	}
	*tasks = append(*tasks, t)
	t = Task{
		Status:      statusCreated,
		Sequence:    1,
		ID:          "DD2",
		ParentID:    "DDD",
		ExecTimeout: 2,
	}
	*tasks = append(*tasks, t)
}
