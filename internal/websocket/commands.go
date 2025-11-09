package websocket

import (
	"time"

	"github.com/gorilla/websocket"
)

// SendCommand envia um comando ao servidor via WebSocket
func SendCommand(conn *websocket.Conn, cmd string) error {
	commandMessage := Message{
		Event: "send command",
		Args:  []string{cmd},
	}
	return conn.WriteJSON(commandMessage)
}

// StopServer para o servidor
func StopServer(conn *websocket.Conn, serverName string) {
	// Enviar comando stop
	SendCommand(conn, "stop")
	time.Sleep(1 * time.Second)
	// Alternativa: tentar parar via power state
	powerMessage := Message{
		Event: "set state",
		Args:  []string{"stop"},
	}
	conn.WriteJSON(powerMessage)
}

