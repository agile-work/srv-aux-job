package controllers

import (
	"fmt"
	"strconv"
)

const (
	paramTypeString  string = "string"
	paramTypeNumber  string = "number"
	paramTypeBoolean string = "boolean"

	paramScopeSystem string = "system"
	paramScopeJob    string = "job"
	paramScopeTask   string = "task"
)

// Param represents a param key value
type Param struct {
	Type      string      `json:"type"`
	Reference string      `json:"ref"`
	Field     string      `json:"field"`
	Key       string      `json:"key"`
	Value     interface{} `json:"value"`
}

func (p *Param) String() string {
	if p.Value == nil {
		return ""
	}

	switch p.Type {
	case paramTypeString:
		return p.Value.(string)
	case paramTypeBoolean:
		return strconv.FormatBool(p.Value.(bool))
	case paramTypeNumber:
		return fmt.Sprintf("%f", p.Value.(float64))
	default:
		return p.Value.(string)
	}
}
