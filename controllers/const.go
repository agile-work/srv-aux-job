package controllers

const (
	statusCreating    string = "Creating"
	statusCreated     string = "Created"
	statusProcessing  string = "Processing"
	statusCompleted   string = "Completed"
	statusWarnings    string = "Warnings"
	statusFail        string = "Fail"
	statusRollbacking string = "Rollbacking"
	statusRetrying    string = "Retrying"

	executeQuery     string = "exec_query"
	executeAPIGet    string = "exec_api_get"
	executeAPIPost   string = "exec_api_post"
	executeAPIDelete string = "exec_api_delete"
	executeAPIUpdate string = "exec_api_update"

	onFailContinue          string = "continue"
	onFailRetryAndContinue  string = "retry_and_continue"
	onFailCancel            string = "cancel"
	onFailRetryAndCancel    string = "retry_and_cancel"
	onFailRollback          string = "rollback"
	onFailRollbackAndCancel string = "rollback_and_cancel"
)
