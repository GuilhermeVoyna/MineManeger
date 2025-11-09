package node

import (
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

// sendCommand envia um comando ao servidor via WebSocket
func sendCommand(conn *websocket.Conn, cmd string) error {
	commandMessage := Message{
		Event: "send command",
		Args:  []string{cmd},
	}
	return conn.WriteJSON(commandMessage)
}

// stopServer para o servidor
func stopServer(conn *websocket.Conn, serverName string) {
	fmt.Printf("\n[%s] [!] Tempo de inatividade esgotado - parando servidor...\n", serverName)
	// Enviar comando stop
	sendCommand(conn, "stop")
	time.Sleep(1 * time.Second)
	// Alternativa: tentar parar via power state
	powerMessage := Message{
		Event: "set state",
		Args:  []string{"stop"},
	}
	conn.WriteJSON(powerMessage)
}

