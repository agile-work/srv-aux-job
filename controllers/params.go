package controllers

const (
	paramTypeSystem string = "system"
	paramTypeSelf   string = "self"
	paramTypeJob    string = "job"
	paramTypeTask   string = "task"
)

// Param represents a param key value
type Param struct {
	Type      string `json:"type"`
	Reference string `json:"ref"`
	Field     string `json:"field"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}
