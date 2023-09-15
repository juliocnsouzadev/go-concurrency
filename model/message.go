package model

type Message struct {
	Id   int    `json:"id,omitempty"`
	Body string `json:"body,omitempty"`
}
