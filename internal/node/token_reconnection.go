package node

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

// reconnectWithNewToken reconecta com um novo token quando o token expira
func reconnectWithNewToken(conn *websocket.Conn, serverId string) (*websocket.Conn, string, error) {
	fmt.Println("[!] Token expirado - reconectando com novo token...")
	
	// Fechar conexão atual
	conn.Close()

	// Obter novo token
	newJwtResponse, err := GetJwt(serverId)
	if err != nil {
		return nil, "", fmt.Errorf("falha ao obter novo token: %v", err)
	}

	newToken := newJwtResponse.Data.Token
	newSocketURL := newJwtResponse.Data.Socket

	if newToken == "" {
		return nil, "", fmt.Errorf("novo token está vazio")
	}

	if newSocketURL == "" {
		return nil, "", fmt.Errorf("nova URL do socket está vazia")
	}

	// Converter URL se necessário
	if strings.HasPrefix(newSocketURL, "https://") {
		newSocketURL = strings.Replace(newSocketURL, "https://", "wss://", 1)
	} else if strings.HasPrefix(newSocketURL, "http://") {
		newSocketURL = strings.Replace(newSocketURL, "http://", "ws://", 1)
	}

	// Criar nova conexão
	fmt.Println("[INFO] Reconectando ao WebSocket com novo token...")
	newHeaders := http.Header{}
	newHeaders.Add("Authorization", "Bearer "+newToken)
	newHeaders.Add("Origin", "https://painel.riguila.com.br")

	newConn, newResp, err := websocket.DefaultDialer.Dial(newSocketURL, newHeaders)
	if err != nil {
		if newResp != nil {
			return nil, "", fmt.Errorf("falha ao reconectar: %v (Status: %d)", err, newResp.StatusCode)
		}
		return nil, "", fmt.Errorf("falha ao reconectar: %v", err)
	}

	// Reautenticar
	authMessage := Message{
		Event: "auth",
		Args:  []string{newToken},
	}

	if err := newConn.WriteJSON(authMessage); err != nil {
		newConn.Close()
		return nil, "", fmt.Errorf("falha ao reautenticar: %v", err)
	}

	fmt.Println("[✓] WebSocket reconectado e reautenticado com sucesso")
	return newConn, newToken, nil
}

