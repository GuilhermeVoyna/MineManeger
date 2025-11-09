package node

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// handleAuthSuccess processa autenticação bem-sucedida
func handleAuthSuccess(conn *websocket.Conn, state *MonitoringState, serverName string) {
	fmt.Printf("[%s] [✓] Autenticação bem-sucedida\n", serverName)
	
	time.Sleep(500 * time.Millisecond)
	if err := sendCommand(conn, "say monitoring"); err != nil {
		fmt.Printf("[%s] [ERRO] Falha ao enviar comando 'say monitoring': %v\n", serverName, err)
	} else {
		fmt.Printf("[%s] [✓] Comando 'say monitoring' enviado\n", serverName)
	}
	
	time.Sleep(1 * time.Second)
	sendCommand(conn, "list")
	
	state.Mutex.Lock()
	state.LastPlayerActivity = time.Now()
	state.InactivityTimer.Reset(state.InactivityTimeout)
	state.Mutex.Unlock()
	
	timeoutStr := formatDuration(state.InactivityTimeout)
	fmt.Printf("[%s] [INFO] Monitoramento iniciado - servidor será parado após %s sem jogadores\n", serverName, timeoutStr)
}

// handleConsoleOutput processa logs do console
func handleConsoleOutput(conn *websocket.Conn, state *MonitoringState, args json.RawMessage, serverName string) {
	// Tentar como array
	var logData []string
	if err := json.Unmarshal(args, &logData); err == nil && len(logData) > 0 {
		processLogLines(conn, state, logData, serverName)
		return
	}

	// Tentar como string simples
	var logLine string
	if err := json.Unmarshal(args, &logLine); err == nil && logLine != "" {
		processLogLines(conn, state, []string{logLine}, serverName)
		return
	}

	// Tentar como objeto
	var logObj map[string]interface{}
	if err := json.Unmarshal(args, &logObj); err == nil {
		if lines, ok := logObj["lines"].([]interface{}); ok {
			var logLines []string
			for _, line := range lines {
				if lineStr, ok := line.(string); ok {
					logLines = append(logLines, lineStr)
				}
			}
			processLogLines(conn, state, logLines, serverName)
		} else if line, ok := logObj["line"].(string); ok && line != "" {
			processLogLines(conn, state, []string{line}, serverName)
		}
	}
}

// processLogLines processa linhas de log
func processLogLines(conn *websocket.Conn, state *MonitoringState, lines []string, serverName string) {
	timestamp := time.Now().Format("15:04:05")
	
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		
		fmt.Printf("[%s] [%s] %s\n", serverName, timestamp, line)

		// Detectar reinício do servidor
		if isServerStarted(line) {
			handleServerRestart(conn, state, serverName)
		}

		// Processar atividade de jogador
		state.processPlayerActivity(conn, line)
	}
}

// handleServerRestart processa reinício do servidor
func handleServerRestart(conn *websocket.Conn, state *MonitoringState, serverName string) {
	state.resetMonitoring()
	fmt.Printf("[%s] [INFO] Servidor reiniciado detectado - reiniciando monitoramento\n", serverName)

	time.Sleep(1 * time.Second)
	sendCommand(conn, "say monitoring...")
	time.Sleep(1 * time.Second)
	sendCommand(conn, "list")

	timeoutStr := formatDuration(state.InactivityTimeout)
	fmt.Printf("[%s] [INFO] Monitoramento reiniciado - servidor será parado após %s sem jogadores\n", serverName, timeoutStr)
}

// handleStatus processa mudanças de status do servidor
func handleStatus(state *MonitoringState, args json.RawMessage, serverName string) bool {
	var status string
	if err := json.Unmarshal(args, &status); err != nil {
		return false
	}

	fmt.Printf("[%s] [STATUS] Servidor: %s\n", serverName, status)
	statusLower := strings.ToLower(status)

	// Detectar se o servidor foi parado
	if statusLower == "stopped" || statusLower == "offline" || statusLower == "stopping" {
		state.Mutex.Lock()
		state.ServerStopped = true
		state.InactivityTimer.Stop()
		state.Mutex.Unlock()
		fmt.Printf("[%s] [INFO] Servidor parado - encerrando leitura do WebSocket\n", serverName)
		return true // Indica que deve parar
	}

	// Detectar se o servidor está iniciando
	if statusLower == "starting" || statusLower == "running" {
		state.Mutex.Lock()
		state.PlayersOnline = 0
		state.LastPlayerActivity = time.Now()
		state.InactivityTimer.Stop()
		state.Mutex.Unlock()
		fmt.Printf("[%s] [INFO] Servidor iniciando/reiniciando - resetando monitoramento\n", serverName)
	}

	return false
}

// handleTokenExpired processa expiração do token
func handleTokenExpired(conn *websocket.Conn, state *MonitoringState, serverName string) (*websocket.Conn, string, error) {
	// Obter serverId do estado
	state.Mutex.Lock()
	serverID := state.ServerID
	state.Mutex.Unlock()
	
	newConn, newToken, err := reconnectWithNewToken(conn, serverID)
	if err != nil {
		return nil, "", err
	}

	state.resetMonitoring()
	fmt.Printf("[%s] [✓] Estado de monitoramento resetado após reconexão\n", serverName)
	return newConn, newToken, nil
}

