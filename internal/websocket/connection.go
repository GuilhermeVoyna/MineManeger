package websocket

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"

	"maneger/internal/api"
)

// GetConnection obtém uma conexão WebSocket para o servidor
func GetConnection(serverID, serverName string) (*websocket.Conn, error) {
	jwtResponse, err := api.GetJwt(serverID)
	if err != nil {
		return nil, fmt.Errorf("falha ao obter JWT: %v", err)
	}

	token := jwtResponse.Data.Token
	socketURL := jwtResponse.Data.Socket

	if token == "" || socketURL == "" {
		return nil, fmt.Errorf("token ou URL do socket vazios")
	}

	// Converter URL para WebSocket
	if strings.HasPrefix(socketURL, "https://") {
		socketURL = strings.Replace(socketURL, "https://", "wss://", 1)
	} else if strings.HasPrefix(socketURL, "http://") {
		socketURL = strings.Replace(socketURL, "http://", "ws://", 1)
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+token)
	headers.Add("Origin", "https://painel.riguila.com.br")

	conn, resp, err := websocket.DefaultDialer.Dial(socketURL, headers)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("erro ao conectar: %v (Status: %d)", err, resp.StatusCode)
		}
		return nil, fmt.Errorf("erro ao conectar: %v", err)
	}

	return conn, nil
}

// Authenticate autentica a conexão WebSocket
func Authenticate(conn *websocket.Conn, token string) error {
	authMessage := Message{
		Event: "auth",
		Args:  []string{token},
	}
	return conn.WriteJSON(authMessage)
}

