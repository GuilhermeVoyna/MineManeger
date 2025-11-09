package node

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// MonitorAllServers inicia monitoramento para todos os servidores disponíveis
func MonitorAllServers() {
	servers, err := ListServers()
	if err != nil {
		fmt.Printf("Error listing servers: %v\n", err)
		return
	}

	if len(servers) == 0 {
		fmt.Println("Error: No servers found")
		return
	}

	fmt.Printf("Encontrados %d servidor(es). Iniciando monitoramento...\n", len(servers))

	// Iniciar uma goroutine para cada servidor
	for _, server := range servers {
		go func(srv Server) {
			WebSocketConnection(srv.ID, srv.Name)
		}(server)
	}

	// Manter o programa rodando indefinidamente
	select {}
}

// WebSocketConnection estabelece e mantém conexão WebSocket com um servidor específico
func WebSocketConnection(serverID string, serverName string) {
	fmt.Printf("[%s] Iniciando conexão WebSocket para servidor: %s\n", serverName, serverID)
	
	jwtResponse, err := GetJwt(serverID)
	if err != nil {
		fmt.Printf("[%s] Error getting JWT: %v\n", serverName, err)
		return
	}

	token := jwtResponse.Data.Token
	socketURL := jwtResponse.Data.Socket

	if token == "" {
		fmt.Printf("[%s] Error: Token is empty\n", serverName)
		return
	}

	if socketURL == "" {
		fmt.Printf("[%s] Error: Socket URL is empty\n", serverName)
		return
	}

	// Converter URL para WebSocket
	if strings.HasPrefix(socketURL, "https://") {
		socketURL = strings.Replace(socketURL, "https://", "wss://", 1)
	} else if strings.HasPrefix(socketURL, "http://") {
		socketURL = strings.Replace(socketURL, "http://", "ws://", 1)
	}

	fmt.Printf("[%s] Connecting to WebSocket: %s\n", serverName, socketURL)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+token)
	headers.Add("Origin", "https://painel.riguila.com.br")

	conn, resp, err := websocket.DefaultDialer.Dial(socketURL, headers)
	if err != nil {
		if resp != nil {
			fmt.Printf("[%s] Error connecting: %v (Status: %d)\n", serverName, err, resp.StatusCode)
		} else {
			fmt.Printf("[%s] Error connecting: %v\n", serverName, err)
		}
		return
	}
	defer conn.Close()

	fmt.Printf("[%s] WebSocket connection established\n", serverName)

	// Autenticar
	authMessage := Message{
		Event: "auth",
		Args:  []string{token},
	}

	if err := conn.WriteJSON(authMessage); err != nil {
		fmt.Printf("[%s] Error sending auth: %v\n", serverName, err)
		return
	}

	fmt.Printf("[%s] Authentication message sent\n", serverName)
	fmt.Printf("[%s] Aguardando logs do servidor...\n", serverName)

	// Inicializar estado de monitoramento
	inactivityTimeout := 10 * time.Second
	state := NewMonitoringState(inactivityTimeout, serverID, serverName)

	// Iniciar monitoramento de inatividade
		state.startInactivityMonitoring(conn, func() {
		stopServer(conn, serverName)
	})

	// Loop principal de mensagens
	for {
		// Verificar se o servidor foi parado
		state.Mutex.Lock()
		stopped := state.ServerStopped
		state.Mutex.Unlock()

		if stopped {
			fmt.Printf("[%s] [INFO] Servidor parado - parando leitura de mensagens\n", serverName)
			break
		}

		var message ServerMessage
		err := conn.ReadJSON(&message)
		if err != nil {
			fmt.Printf("[%s] Erro ao ler mensagem: %v\n", serverName, err)
			break
		}

		// Processar eventos
		switch message.Event {
		case "auth success":
			handleAuthSuccess(conn, state, serverName)

		case "console output":
			handleConsoleOutput(conn, state, message.Args, serverName)

		case "stats":
			// Estatísticas do servidor (não processadas por padrão)

		case "status":
			if handleStatus(state, message.Args, serverName) {
				return // Servidor parado
			}

		case "token expiring":
			fmt.Printf("[%s] [!] Token expirando em breve - preparando reconexão...\n", serverName)

		case "token expired":
			newConn, newToken, err := handleTokenExpired(conn, state, serverName)
			if err != nil {
				fmt.Printf("[%s] [ERRO] %v\n", serverName, err)
				return
			}
			conn = newConn
			token = newToken
			continue

		case "daemon error":
			var errorMsg string
			if err := json.Unmarshal(message.Args, &errorMsg); err == nil {
				fmt.Printf("[%s] [ERRO] Daemon: %s\n", serverName, errorMsg)
			}

		case "install output":
			var installLog string
			if err := json.Unmarshal(message.Args, &installLog); err == nil {
				timestamp := time.Now().Format("15:04:05")
				fmt.Printf("[%s] [%s] [INSTALL] %s\n", serverName, timestamp, installLog)
			}

		case "install started":
			fmt.Printf("[%s] [INSTALL] Instalação iniciada\n", serverName)

		case "install completed":
			fmt.Printf("[%s] [INSTALL] Instalação concluída\n", serverName)
		}
	}
}
