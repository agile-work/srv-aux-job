package controllers

import (
	"fmt"
	"time"
)

//Task represents an task instance that need to be executed in the Job instance
type Task struct {
	ID               string    `json:"id"`
	Code             string    `json:"code"`
	JobInstanceID    string    `json:"job_instance_id"`
	Status           string    `json:"status"`
	StartAt          time.Time `json:"start_at"`
	FinishAt         time.Time `json:"finish_at"`
	Sequence         int       `json:"task_sequence"`
	ParentID         string    `json:"parent_id"`
	ExecTimeout      int       `json:"exec_timeout"`
	ExecAction       string    `json:"exec_action"`
	ExecAddress      string    `json:"exec_address"`
	ExecPayload      string    `json:"exec_payload"`
	ExecResponse     string    `json:"exec_response"`
	ActionOnFail     string    `json:"action_on_fail"`
	MaxRetryAttempts int       `json:"max_retry_attempts"`
	RollbackAction   string    `json:"rollback_action"`
	RollbackAddress  string    `json:"rollback_address"`
	RollbackPayload  string    `json:"rollback_payload"`
	RollbackResponse string    `json:"rollback_response"`
	Params           []Param
	retryAttempts    int
}

//Run executes the task
func (t *Task) Run(responses chan<- *Task) {
	t.Status = statusProcessing
	fmt.Printf("    Task: %s | Status: %s\n", t.ID, t.Status)
	t.StartAt = time.Now()
	//TODO: update task on the DB
	time.Sleep(time.Duration(t.ExecTimeout) * time.Second)
	t.Status = statusCompleted
	t.FinishAt = time.Now()
	responses <- t
}

//LoadParams fetch task params from the database
func (t *Task) LoadParams() {
	//TODO: Load task params from the DB
}
