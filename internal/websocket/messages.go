package websocket

import "encoding/json"

type Message struct {
	Event string   `json:"event"`
	Args  []string `json:"args"`
}

type ServerMessage struct {
	Event string          `json:"event"`
	Args  json.RawMessage `json:"args"`
	Data  json.RawMessage `json:"data"`
}

