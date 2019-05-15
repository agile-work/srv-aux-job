package controllers

const (
	statusCreating    string = "creating"
	statusCreated     string = "created"
	statusInQueue     string = "queued"
	statusProcessing  string = "processing"
	statusCompleted   string = "completed"
	statusWarnings    string = "warnings"
	statusFail        string = "fail"
	statusRollbacking string = "rollbacking"
	statusRetrying    string = "retrying"

	executeQuery     string = "exec_query"
	executeAPIGet    string = "api_get"
	executeAPIPost   string = "api_post"
	executeAPIDelete string = "api_delete"
	executeAPIUpdate string = "api_patch"

	onFailContinue          string = "continue"
	onFailRetryAndContinue  string = "retry_and_continue"
	onFailCancel            string = "cancel"
	onFailRetryAndCancel    string = "retry_and_cancel"
	onFailRollback          string = "rollback"
	onFailRollbackAndCancel string = "rollback_and_cancel"
)
