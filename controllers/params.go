package controllers

//Param represents an task param key value
type Param struct {
	ID        string `json:"id" sql:"id"`
	Type      string `json:"param_type" sql:"param_type"`
	Reference string `json:"param_ref" sql:"param_ref"`
	Key       string `json:"param_key" sql:"param_key"`
	Value     string `json:"param_value" sql:"param_value"`
}
