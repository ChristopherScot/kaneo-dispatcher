// Package protocol defines the dispatcher/worker wire contract.
package protocol

import "encoding/json"

const Version = 1

// Payload is a JSON string so its exact hashed bytes survive HTTP/JSON and
// ConfigMap round trips. Never hash a client's reserialization of this JSON.
type Prompt struct {
	Template       string `json:"template"`
	TemplateDigest string `json:"templateDigest"`
	Digest         string `json:"digest"`
	Payload        string `json:"payload,omitempty"`
}

type Task struct {
	ID          string          `json:"id"`
	Project     string          `json:"projectId"`
	Title       string          `json:"title"`
	Description json.RawMessage `json:"description"`
}
type Comment struct {
	Type      string `json:"type"`
	Content   string `json:"content"`
	CreatedAt string `json:"createdAt"`
}

type Payload struct {
	Version        int       `json:"version"`
	Task           Task      `json:"task"`
	Repo           string    `json:"repo"`
	Stage          string    `json:"stage"`
	Mode           string    `json:"mode"`
	Branch         string    `json:"branch"`
	Base           string    `json:"base"`
	BaseSHA        string    `json:"baseSHA"`
	BranchSHA      string    `json:"branchSHA"`
	PR             int       `json:"pr"`
	HeadSHA        string    `json:"headSHA"`
	Reworks        int       `json:"reworks"`
	TimeoutSeconds int       `json:"timeoutSeconds"`
	Result         string    `json:"result"`
	Instructions   string    `json:"instructions"`
	Comments       []Comment `json:"comments"`
}

type Report struct {
	NoteKind     string `json:"noteKind,omitempty"`
	PromptDigest string `json:"promptDigest,omitempty"`
	Message      string `json:"message,omitempty"`
	URL          string `json:"url,omitempty"`
	Title        string `json:"title,omitempty"`
	PR           int    `json:"pr,omitempty"`
	SHA          string `json:"sha,omitempty"`
	Task         string `json:"task"`
	Command      string `json:"command"`
	Event        string `json:"event"`
	RevertPR     int    `json:"revertPR,omitempty"`
}

type Request struct {
	Version int    `json:"version"`
	Task    string `json:"task"`
	Command string `json:"command"`
}
type Command struct {
	Version int    `json:"version"`
	Task    string `json:"task"`
	Command string `json:"command"`
	Repo    string `json:"repo"`
	Stage   string `json:"stage"`
	Prompt  Prompt `json:"prompt"`
}
type Ack struct {
	Version  int    `json:"version"`
	Task     string `json:"task"`
	Command  string `json:"command"`
	Event    string `json:"event"`
	Accepted bool   `json:"accepted"`
}
