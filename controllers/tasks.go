package controllers

import (
	"bytes"
	"crypto/tls"
	"io/ioutil"
	"net/http"
	"regexp"
	"time"

	"github.com/agile-work/srv-shared/constants"
	"github.com/agile-work/srv-shared/sql-builder/builder"
	"github.com/agile-work/srv-shared/sql-builder/db"

	"github.com/tidwall/gjson"
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
func (t *Task) Run(responses chan<- *Task, token string) {
	t.Status = constants.JobStatusProcessing
	t.StartAt = time.Now()
	opt := &db.Options{Conditions: builder.Equal("id", t.ID)}

	db.UpdateStruct(constants.TableCoreJobTaskInstances, t, opt)

	switch t.ExecAction {
	case constants.ExecuteQuery:
		t.executeQuery()
	case constants.ExecuteAPIGet, constants.ExecuteAPIPost, constants.ExecuteAPIUpdate, constants.ExecuteAPIDelete:
		t.executeAPI(token)
	default:
		time.Sleep(time.Duration(t.ExecTimeout) * time.Second)
	}

	t.FinishAt = time.Now()
	db.UpdateStruct(constants.TableCoreJobTaskInstances, t, opt)

	//TODO: if status = fail implement retry and rollback actions

	responses <- t
}

func (t *Task) getParamValue(key string) string {
	for _, p := range t.Params {
		if key == p.Key {
			return p.String()
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
	err := db.Exec(builder.Raw(t.ExecPayload))
	if err != nil {
		t.Status = constants.JobStatusFail
		t.ExecResponse = err.Error()
	} else {
		t.Status = constants.JobStatusCompleted
	}
}

func (t *Task) executeAPI(token string) {
	method := ""
	switch t.ExecAction {
	case constants.ExecuteAPIGet:
		method = http.MethodGet
	case constants.ExecuteAPIPost:
		method = http.MethodPost
	case constants.ExecuteAPIUpdate:
		method = http.MethodPatch
	case constants.ExecuteAPIDelete:
		method = http.MethodDelete
	}

	timeout := time.Duration(t.ExecTimeout) * time.Second
	client := http.Client{
		Timeout: timeout,
	}

	// TODO: Retirar quando o certificado estiver ok
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	request, err := http.NewRequest(method, t.ExecAddress, bytes.NewBuffer([]byte(t.ExecPayload)))
	request.Header.Set("Content-type", "application/json")
	request.Header.Set("Authorization", token)

	if err != nil {
		t.Status = constants.JobStatusFail
		return
	}

	resp, err := client.Do(request)
	if err != nil {
		t.Status = constants.JobStatusFail
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Status = constants.JobStatusFail
		return
	}

	t.parseResponseToParams(body)

	t.ExecResponse = string(body)
	t.Status = constants.JobStatusCompleted
}

func (t *Task) parseResponseToParams(response []byte) {
	respString := string(response)
	for i, p := range t.Params {
		val := gjson.Get(respString, p.Field)
		switch p.Type {
		case paramTypeString:
			t.Params[i].Value = val.String()
		case paramTypeBoolean:
			t.Params[i].Value = val.Bool()
		case paramTypeNumber:
			t.Params[i].Value = val.Float()
		default:
			t.Params[i].Value = val.String()
		}
	}
}
