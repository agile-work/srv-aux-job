package controllers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"time"
)

// Task represents an task instance that need to be executed in the Job instance
type Task struct {
	ID               string    `json:"id" sql:"id" pk:"true"`
	TaskID           string    `json:"task_id" sql:"task_id"`
	JobInstanceID    string    `json:"job_instance_id" sql:"job_instance_id"`
	Code             string    `json:"code" sql:"code"`
	Status           string    `json:"status" sql:"status"`
	StartAt          time.Time `json:"start_at" sql:"start_at"`
	FinishAt         time.Time `json:"finish_at" sql:"finish_at"`
	Sequence         int       `json:"task_sequence" sql:"task_sequence"`
	ParentID         string    `json:"parent_id" sql:"parent_id"`
	ExecTimeout      int       `json:"exec_timeout" sql:"exec_timeout"`
	ExecAction       string    `json:"exec_action" sql:"exec_action"`
	ExecAddress      string    `json:"exec_address" sql:"exec_address"`
	ExecPayload      string    `json:"exec_payload" sql:"exec_payload"`
	ExecResponse     string    `json:"exec_response" sql:"exec_response"`
	ActionOnFail     string    `json:"action_on_fail" sql:"action_on_fail"`
	MaxRetryAttempts int       `json:"max_retry_attempts" sql:"max_retry_attempts"`
	RollbackAction   string    `json:"rollback_action" sql:"rollback_action"`
	RollbackAddress  string    `json:"rollback_address" sql:"rollback_address"`
	RollbackPayload  string    `json:"rollback_payload" sql:"rollback_payload"`
	RollbackResponse string    `json:"rollback_response" sql:"rollback_response"`
	Params           []Param   `json:"parameters" sql:"parameters" field:"jsonb"`
	retryAttempts    int
}

// Run executes the task
func (t *Task) Run(responses chan<- *Task) {
	fmt.Printf("    Task: %s | Status: %s\n", t.ID, t.Status)
	t.StartAt = time.Now()

	// db.UpdateStruct(shared.TableCoreJobInstanceTasks, t, builder.Equal("id", t.ID))

	// switch t.ExecAction {
	// case executeQuery:
	// 	t.executeQuery()
	// case executeAPIGet, executeAPIPost, executeAPIUpdate, executeAPIDelete:
	// 	t.executeAPI()
	// default:
	// 	time.Sleep(time.Duration(t.ExecTimeout) * time.Second)
	// }
	fmt.Println("")
	fmt.Println(t.ExecAction)
	fmt.Println(t.ExecAddress)
	fmt.Println(t.ExecPayload)
	fmt.Println("")
	if len(t.Params) > 0 {
		t.Params[0].Value = "ID000001"
	}
	t.Status = statusCompleted
	t.FinishAt = time.Now()
	//db.UpdateStruct(shared.TableCoreJobInstanceTasks, t, builder.Equal("id", t.ID))

	//TODO: if status = fail implement retry and rollback actions

	responses <- t
	fmt.Printf("    Task: %s | Status: %s\n", t.ID, t.Status)
}

func (t *Task) getParamValue(key string) string {
	for _, p := range t.Params {
		if key == p.Key {
			return p.Value
		}
	}
	return ""
}

func (t *Task) getReferenceParams() []string {
	r, _ := regexp.Compile("{([a-z.0-9_]+)}")
	param := []string{}

	param = append(param, r.FindAllString(t.ExecAddress, -1)...)
	param = append(param, r.FindAllString(t.ExecPayload, -1)...)
	//TODO: check rollback address and payload for params

	return param
}

func (t *Task) executeQuery() {
	t.Status = statusCompleted
}

func (t *Task) executeAPI() {
	method := ""
	switch t.ExecAction {
	case executeAPIGet:
		method = http.MethodGet
	case executeAPIPost:
		method = http.MethodPost
	case executeAPIUpdate:
		method = http.MethodPatch
	case executeAPIDelete:
		method = http.MethodDelete
	}

	timeout := time.Duration(t.ExecTimeout) * time.Second
	client := http.Client{
		Timeout: timeout,
	}

	request, err := http.NewRequest(method, t.ExecAddress, bytes.NewBuffer([]byte(t.ExecPayload)))
	request.Header.Set("Content-type", "application/json")
	if err != nil {
		t.Status = statusFail
		return
	}

	resp, err := client.Do(request)
	if err != nil {
		t.Status = statusFail
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Status = statusFail
		return
	}

	t.parseResponseToSelfParams(body)

	t.Status = statusCompleted
}

func (t *Task) parseResponseToSelfParams(response []byte) {
	jsonMap := make(map[string]interface{})
	json.Unmarshal(response, &jsonMap)

	for i, p := range t.Params {
		if p.Type == paramTypeSelf {
			if val, ok := jsonMap[p.Field]; ok {
				t.Params[i].Value = val.(string)
			}
		}
	}
}
