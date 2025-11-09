package monitor

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"maneger/internal/api"
	"maneger/internal/detection"
	"maneger/internal/fakeserver"
	ws "maneger/internal/websocket"
)

// ServerMonitor gerencia o monitoramento de todos os servidores
type ServerMonitor struct {
	// Configuração
	checkInterval     time.Duration // Intervalo para verificar servidores
	inactivityTimeout time.Duration // Tempo de inatividade antes de parar

	// Estado dos servidores
	servers map[string]*ServerTracker
	mutex   sync.RWMutex

	// Controle
	stopChan chan struct{}
}

// ServerTracker rastreia o estado de um servidor
type ServerTracker struct {
	// Informações do servidor
	ServerID   string
	ServerName string

	// Estado atual
	IsOnline        bool
	APIServerStatus string // Status retornado pela API
	PlayersOnline   int
	LastActivity    time.Time

	// Timer de inatividade
	InactivityTimerStart time.Time
	HasTimer             bool

	// Controle de comandos
	ListCommandSent     bool      // Flag para evitar enviar comando list múltiplas vezes
	LastListCommandTime time.Time // Última vez que o comando list foi enviado

	// Conexão WebSocket
	conn      *websocket.Conn
	connMutex sync.RWMutex

	// Fake server
	fakeServer      *fakeserver.FakeServer
	fakeServerPort  int
	fakeServerMutex sync.RWMutex

	// Mutex para thread-safety
	mutex sync.RWMutex
}

// NewServerMonitor cria um novo monitor de servidores
func NewServerMonitor(checkInterval, inactivityTimeout time.Duration) *ServerMonitor {
	return &ServerMonitor{
		checkInterval:     checkInterval,
		inactivityTimeout: inactivityTimeout,
		servers:           make(map[string]*ServerTracker),
		stopChan:          make(chan struct{}),
	}
}

// Start inicia o monitoramento periódico
func (sm *ServerMonitor) Start() {
	// Primeira verificação imediata
	sm.checkAllServers()

	// Criar ticker para verificações periódicas
	ticker := time.NewTicker(sm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sm.checkAllServers()
		case <-sm.stopChan:
			sm.closeAllConnections()
			return
		}
	}
}

// Stop para o monitoramento
func (sm *ServerMonitor) Stop() {
	close(sm.stopChan)
}

// checkAllServers verifica todos os servidores (passo 1)
func (sm *ServerMonitor) checkAllServers() {
	// 1. Verificar quais servidores existem
	servers, err := api.ListServers()
	if err != nil {
		return
	}

	if len(servers) == 0 {
		return
	}

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	// Para cada servidor, garantir que temos conexão WebSocket para obter status
	for _, server := range servers {
		tracker := sm.getOrCreateTracker(server.ID, server.Name)

		// Verificar se já temos uma conexão WebSocket ativa
		tracker.connMutex.RLock()
		hasConnection := tracker.conn != nil
		tracker.connMutex.RUnlock()

		// Se não temos conexão, conectar para obter status via WebSocket
		if !hasConnection {
			go sm.connectAndMonitorServer(tracker)
		}

		// Também verificar status via Client API como fallback
		go sm.checkServerStatusViaAPI(tracker)

		// Verificar se precisa iniciar/parar fake server baseado no status
		tracker.mutex.RLock()
		isOnline := tracker.IsOnline
		tracker.mutex.RUnlock()

		if !isOnline {
			// Servidor está offline - garantir que fake server está rodando na mesma porta
			sm.ensureFakeServerRunning(tracker)
		} else {
			// Servidor está online - garantir que fake server está parado
			sm.stopFakeServer(tracker)
		}
	}

	// Mostrar status de todos os servidores
	sm.printServersStatus()

	// 4. Verificar TTL dos timers (passo 4) e parar servidores expirados (passo 5)
	sm.checkTimersTTL()
}

// printServersStatus imprime o status de todos os servidores
func (sm *ServerMonitor) printServersStatus() {
	fmt.Println("\n[MONITOR] Status dos servidores:")
	fmt.Println("┌─────────────────────────────────────────────────────────────────┐")
	fmt.Printf("│ %-20s │ %-10s │ %-8s │ %-15s │\n", "Nome", "Status", "Players", "TTL Restante")
	fmt.Println("├─────────────────────────────────────────────────────────────────┤")

	now := time.Now()
	for _, tracker := range sm.servers {
		tracker.mutex.RLock()
		name := tracker.ServerName
		isOnline := tracker.IsOnline
		players := tracker.PlayersOnline
		hasTimer := tracker.HasTimer
		timerStart := tracker.InactivityTimerStart
		tracker.mutex.RUnlock()

		// Determinar status - usar o status do WebSocket
		tracker.mutex.RLock()
		wsStatus := tracker.APIServerStatus
		tracker.mutex.RUnlock()

		status := "offline"
		if isOnline {
			status = "online"
		}

		// Mostrar status do WebSocket se disponível
		if wsStatus != "" {
			status = fmt.Sprintf("%s (%s)", status, wsStatus)
		} else {
			// Se ainda não recebemos status do WebSocket, mostrar "aguardando"
			status = "aguardando status..."
		}

		// Calcular TTL restante
		ttlStr := "-"
		if hasTimer && isOnline {
			elapsed := now.Sub(timerStart)
			remaining := sm.inactivityTimeout - elapsed
			if remaining > 0 {
				ttlStr = formatDuration(remaining)
			} else {
				ttlStr = "EXPIRADO"
			}
		}

		fmt.Printf("│ %-20s │ %-10s │ %-8d │ %-15s │\n", name, status, players, ttlStr)
	}
	fmt.Println("└─────────────────────────────────────────────────────────────────┘")
}

// getOrCreateTracker obtém ou cria um tracker para o servidor
func (sm *ServerMonitor) getOrCreateTracker(serverID, serverName string) *ServerTracker {
	if tracker, exists := sm.servers[serverID]; exists {
		return tracker
	}

	tracker := &ServerTracker{
		ServerID:     serverID,
		ServerName:   serverName,
		IsOnline:     false,
		LastActivity: time.Now(),
	}
	sm.servers[serverID] = tracker
	return tracker
}

// connectAndMonitorServer conecta ao servidor e inicia monitoramento
func (sm *ServerMonitor) connectAndMonitorServer(tracker *ServerTracker) {
	conn, err := ws.GetConnection(tracker.ServerID, tracker.ServerName)
	if err != nil {
		fmt.Printf("[%s] [ERRO] Falha ao conectar WebSocket: %v\n", tracker.ServerName, err)
		return
	}

	tracker.connMutex.Lock()
	tracker.conn = conn
	tracker.connMutex.Unlock()

	// Obter token JWT para autenticação
	jwtResponse, err := api.GetJwt(tracker.ServerID)
	if err != nil {
		fmt.Printf("[%s] [ERRO] Falha ao obter JWT: %v\n", tracker.ServerName, err)
		return
	}

	// Autenticar
	if err := ws.Authenticate(conn, jwtResponse.Data.Token); err != nil {
		fmt.Printf("[%s] [ERRO] Falha ao autenticar: %v\n", tracker.ServerName, err)
		return
	}

	// Iniciar goroutine para ler mensagens
	go sm.readWebSocketMessages(tracker)

	// Iniciar goroutine para verificação periódica de status e jogadores (a cada 5 segundos)
	go sm.periodicStatusCheck(tracker)

	// O comando list será enviado automaticamente apenas uma vez quando o servidor ficar online
	// através do evento de status do WebSocket
}

// checkServerPlayers verifica a quantidade de players (passo 2)
// force: se true, força o envio mesmo se foi enviado recentemente
func (sm *ServerMonitor) checkServerPlayers(tracker *ServerTracker, force bool) {
	tracker.connMutex.RLock()
	conn := tracker.conn
	tracker.connMutex.RUnlock()

	if conn == nil {
		return
	}

	tracker.mutex.Lock()
	// Verificar se já enviamos o comando list recentemente (últimos 5 segundos)
	// Se force=true, sempre enviar
	shouldSend := force || !tracker.ListCommandSent || time.Since(tracker.LastListCommandTime) > 5*time.Second
	if shouldSend {
		tracker.ListCommandSent = true
		tracker.LastListCommandTime = time.Now()
		tracker.mutex.Unlock()

		// Enviar comando list para verificar players
		if err := ws.SendCommand(conn, "list"); err != nil {
			fmt.Printf("[%s] [ERRO] Falha ao enviar comando list: %v\n", tracker.ServerName, err)
			tracker.mutex.Lock()
			tracker.ListCommandSent = false // Permitir tentar novamente em caso de erro
			tracker.mutex.Unlock()
			return
		}

	} else {
		tracker.mutex.Unlock()
		// Já enviamos recentemente, não enviar novamente
		return
	}

	// A resposta será processada em readWebSocketMessages
}

// periodicStatusCheck verifica status periodicamente (a cada 5 segundos)
// NÃO verifica jogadores - apenas observa eventos join/leave
func (sm *ServerMonitor) periodicStatusCheck(tracker *ServerTracker) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Verificar se o servidor está online
			tracker.mutex.RLock()
			isOnline := tracker.IsOnline
			tracker.mutex.RUnlock()

			if isOnline {
				// Apenas verificar status via API (não verificar jogadores)
				go sm.checkServerStatusViaAPI(tracker)
			}
		}
	}
}

// readWebSocketMessages lê mensagens do WebSocket e processa respostas
func (sm *ServerMonitor) readWebSocketMessages(tracker *ServerTracker) {
	tracker.connMutex.RLock()
	conn := tracker.conn
	tracker.connMutex.RUnlock()

	if conn == nil {
		return
	}

	for {
		var message ws.ServerMessage
		err := conn.ReadJSON(&message)
		if err != nil {
			fmt.Printf("[%s] [ERRO] Erro ao ler mensagem WebSocket: %v\n", tracker.ServerName, err)
			// Fechar conexão e marcar como offline
			sm.closeServerConnection(tracker)
			tracker.mutex.Lock()
			tracker.IsOnline = false
			tracker.mutex.Unlock()
			return
		}

		// Processar eventos
		switch message.Event {
		case "auth success":
			// Após autenticação, verificar status via API
			time.Sleep(500 * time.Millisecond)
			go sm.checkServerStatusViaAPI(tracker)

		case "status":
			// Evento de status do servidor
			sm.handleStatusEvent(tracker, message.Args)

		case "stats":
			// Evento de estatísticas - apenas extrair status, sem logs
			sm.handleStatsEvent(tracker, message.Args)

		case "console output":
			// Processar logs do console para detectar resposta do comando "list"
			sm.processConsoleOutput(tracker, message.Args)

		case "token expiring":
			// Token está expirando - renovar antes de expirar
			sm.renewToken(tracker)

		case "token expired":
			sm.closeServerConnection(tracker)
			time.Sleep(2 * time.Second)
			go sm.connectAndMonitorServer(tracker)
			return

		case "jwt error":
			sm.closeServerConnection(tracker)
			time.Sleep(2 * time.Second)
			go sm.connectAndMonitorServer(tracker)
			return
		}
	}
}

// handleStatusEvent processa eventos de status do WebSocket
func (sm *ServerMonitor) handleStatusEvent(tracker *ServerTracker, args json.RawMessage) {
	// Tentar diferentes formatos de parsing
	var status string

	// Formato 1: string simples
	if err := json.Unmarshal(args, &status); err == nil && status != "" {
		sm.updateServerStatus(tracker, status)
		return
	}

	// Formato 2: objeto com campo "status"
	var statusObj map[string]interface{}
	if err := json.Unmarshal(args, &statusObj); err == nil {
		if s, ok := statusObj["status"].(string); ok && s != "" {
			sm.updateServerStatus(tracker, s)
			return
		}
		if s, ok := statusObj["state"].(string); ok && s != "" {
			sm.updateServerStatus(tracker, s)
			return
		}
	}

	// Formato 3: array com string
	var statusArray []string
	if err := json.Unmarshal(args, &statusArray); err == nil && len(statusArray) > 0 {
		sm.updateServerStatus(tracker, statusArray[0])
		return
	}
}

// updateServerStatus atualiza o status do servidor
func (sm *ServerMonitor) updateServerStatus(tracker *ServerTracker, status string) {
	statusLower := strings.ToLower(status)

	// Determinar se está online baseado no status do WebSocket
	isOnline := statusLower == "running" || statusLower == "starting"

	tracker.mutex.Lock()
	wasOnline := tracker.IsOnline
	tracker.IsOnline = isOnline
	tracker.APIServerStatus = status
	tracker.mutex.Unlock()

	if isOnline && !wasOnline {
		// Servidor acabou de ficar online - fechar fake server e resetar flag
		sm.stopFakeServer(tracker)
		tracker.mutex.Lock()
		tracker.ListCommandSent = false
		tracker.mutex.Unlock()
		time.Sleep(1 * time.Second)
		sm.checkServerPlayers(tracker, false)
	} else if !isOnline && wasOnline {
		// Servidor ficou offline - iniciar fake server (ou reiniciar se necessário)
		sm.stopFakeServer(tracker)         // Garantir que não há fake server antigo
		time.Sleep(100 * time.Millisecond) // Pequeno delay para garantir que porta está livre
		sm.startFakeServer(tracker)
		// Limpar timer e resetar flag
		tracker.mutex.Lock()
		tracker.HasTimer = false
		tracker.InactivityTimerStart = time.Time{}
		tracker.PlayersOnline = 0
		tracker.ListCommandSent = false
		tracker.mutex.Unlock()
	} else if !isOnline && !wasOnline {
		// Servidor ainda está offline - garantir que fake server está rodando
		sm.ensureFakeServerRunning(tracker)
	} else if isOnline {
		// Servidor já estava online - verificar se precisa iniciar timer
		tracker.mutex.Lock()
		players := tracker.PlayersOnline
		if players == 0 && !tracker.HasTimer {
			tracker.HasTimer = true
			tracker.InactivityTimerStart = time.Now()
		}
		tracker.mutex.Unlock()
	}
}

// handleStatsEvent processa eventos de estatísticas do WebSocket
// Apenas extrai o status, ignora CPU/memória/network
func (sm *ServerMonitor) handleStatsEvent(tracker *ServerTracker, args json.RawMessage) {
	// O evento stats vem como array com uma string JSON dentro
	var statsArray []string
	if err := json.Unmarshal(args, &statsArray); err != nil {
		return
	}

	if len(statsArray) == 0 {
		return
	}

	// Fazer unmarshal da string JSON dentro do array
	var stats map[string]interface{}
	if err := json.Unmarshal([]byte(statsArray[0]), &stats); err != nil {
		return
	}

	// Apenas extrair status, ignorar CPU/memória/network
	if state, ok := stats["state"].(string); ok && state != "" {
		sm.updateServerStatus(tracker, state)
	}
}

// processConsoleOutput processa saída do console para detectar contagem de players
func (sm *ServerMonitor) processConsoleOutput(tracker *ServerTracker, args json.RawMessage) {
	// Tentar parsear como string
	var logLine string
	if err := json.Unmarshal(args, &logLine); err == nil {
		// Verificar se é resposta do comando "list" (apenas uma vez quando servidor fica online)
		if count, ok := detection.ParseListCommand(logLine); ok {
			sm.updatePlayerCount(tracker, count)
			return
		}

		// Verificar se jogador entrou - incrementar contagem
		if detection.CheckPlayerJoin(logLine) {
			tracker.mutex.Lock()
			tracker.PlayersOnline++
			tracker.LastActivity = time.Now()
			// Cancelar timer se havia jogadores
			if tracker.HasTimer {
				tracker.HasTimer = false
				tracker.InactivityTimerStart = time.Time{}
			}
			newCount := tracker.PlayersOnline
			tracker.mutex.Unlock()
			fmt.Printf("[%s] Jogadores: %d\n", tracker.ServerName, newCount)
			return
		}

		// Verificar se jogador saiu - decrementar contagem
		if detection.CheckPlayerLeave(logLine) {
			tracker.mutex.Lock()
			if tracker.PlayersOnline > 0 {
				tracker.PlayersOnline--
				tracker.LastActivity = time.Now()
				// Se não há mais jogadores e servidor está online, iniciar timer
				if tracker.PlayersOnline == 0 && tracker.IsOnline && !tracker.HasTimer {
					tracker.HasTimer = true
					tracker.InactivityTimerStart = time.Now()
				}
			}
			newCount := tracker.PlayersOnline
			tracker.mutex.Unlock()
			if newCount >= 0 {
				fmt.Printf("[%s] Jogadores: %d\n", tracker.ServerName, newCount)
			}
			return
		}
		return
	}

	// Tentar parsear como array
	var logLines []string
	if err := json.Unmarshal(args, &logLines); err == nil {
		for _, line := range logLines {
			// Verificar se é resposta do comando "list" (apenas uma vez quando servidor fica online)
			if count, ok := detection.ParseListCommand(line); ok {
				sm.updatePlayerCount(tracker, count)
				break
			}

			// Verificar se jogador entrou - incrementar contagem
			if detection.CheckPlayerJoin(line) {
				tracker.mutex.Lock()
				tracker.PlayersOnline++
				tracker.LastActivity = time.Now()
				if tracker.HasTimer {
					tracker.HasTimer = false
					tracker.InactivityTimerStart = time.Time{}
				}
				newCount := tracker.PlayersOnline
				tracker.mutex.Unlock()
				fmt.Printf("[%s] Jogadores: %d\n", tracker.ServerName, newCount)
				break
			}

			// Verificar se jogador saiu - decrementar contagem
			if detection.CheckPlayerLeave(line) {
				tracker.mutex.Lock()
				if tracker.PlayersOnline > 0 {
					tracker.PlayersOnline--
					tracker.LastActivity = time.Now()
					if tracker.PlayersOnline == 0 && tracker.IsOnline && !tracker.HasTimer {
						tracker.HasTimer = true
						tracker.InactivityTimerStart = time.Now()
					}
				}
				newCount := tracker.PlayersOnline
				tracker.mutex.Unlock()
				if newCount >= 0 {
					fmt.Printf("[%s] Jogadores: %d\n", tracker.ServerName, newCount)
				}
				break
			}
		}
	}
}

// updatePlayerCount atualiza a contagem de players e aplica timer se necessário (passo 3)
func (sm *ServerMonitor) updatePlayerCount(tracker *ServerTracker, count int) {
	tracker.mutex.Lock()
	oldCount := tracker.PlayersOnline
	tracker.PlayersOnline = count
	tracker.LastActivity = time.Now()
	tracker.mutex.Unlock()

	if oldCount != count {
		fmt.Printf("[%s] Jogadores: %d\n", tracker.ServerName, count)
	}

	// Aplicar timer se necessário (passo 3)
	tracker.mutex.Lock()
	if count == 0 && !tracker.HasTimer && tracker.IsOnline {
		// Não há players e servidor está online - iniciar timer
		tracker.HasTimer = true
		tracker.InactivityTimerStart = time.Now()
	} else if count > 0 && tracker.HasTimer {
		// Há players - cancelar timer
		tracker.HasTimer = false
		tracker.InactivityTimerStart = time.Time{}
	} else if count == 0 && tracker.HasTimer && tracker.IsOnline {
		// Ainda sem players - reiniciar timer
		tracker.InactivityTimerStart = time.Now()
	}
	tracker.mutex.Unlock()
}

// checkServerStatusViaAPI verifica o status do servidor via Client API
func (sm *ServerMonitor) checkServerStatusViaAPI(tracker *ServerTracker) {
	status, err := api.GetServerStatus(tracker.ServerID)
	if err != nil {
		// Erro ao obter status - não fazer nada, usar WebSocket
		return
	}

	if status != "" {
		statusLower := strings.ToLower(status)
		isOnline := statusLower == "running" || statusLower == "starting"

		tracker.mutex.Lock()
		wasOnline := tracker.IsOnline
		// Atualizar status (API tem prioridade se WebSocket ainda não enviou)
		if tracker.APIServerStatus == "" || tracker.APIServerStatus != status {
			tracker.IsOnline = isOnline
			tracker.APIServerStatus = status
		}
		players := tracker.PlayersOnline
		tracker.mutex.Unlock()

		if isOnline && !wasOnline {
			// Servidor acabou de ficar online - fechar fake server
			sm.stopFakeServer(tracker)
			// Resetar flag para permitir enviar comando list
			tracker.mutex.Lock()
			tracker.ListCommandSent = false
			tracker.mutex.Unlock()
			// Verificar players se acabou de ficar online
			time.Sleep(1 * time.Second)
			sm.checkServerPlayers(tracker, false)
		} else if !isOnline && !wasOnline {
			// Servidor ainda está offline - garantir que fake server está rodando
			sm.ensureFakeServerRunning(tracker)
		} else if !isOnline && wasOnline {
			// Servidor acabou de ficar offline via API - iniciar fake server
			sm.stopFakeServer(tracker)         // Garantir que não há fake server antigo
			time.Sleep(100 * time.Millisecond) // Pequeno delay para garantir que porta está livre
			sm.startFakeServer(tracker)
		} else if isOnline && players == 0 && !tracker.HasTimer {
			// Servidor online sem players e sem timer - iniciar timer
			tracker.mutex.Lock()
			tracker.HasTimer = true
			tracker.InactivityTimerStart = time.Now()
			tracker.mutex.Unlock()
		}
	}
}

// renewToken renova o token quando está expirando
func (sm *ServerMonitor) renewToken(tracker *ServerTracker) {
	tracker.connMutex.RLock()
	conn := tracker.conn
	tracker.connMutex.RUnlock()

	if conn == nil {
		return
	}

	// Obter novo token
	jwtResponse, err := api.GetJwt(tracker.ServerID)
	if err != nil {
		return
	}

	// Reautenticar com o novo token
	tracker.connMutex.Lock()
	if tracker.conn != nil {
		ws.Authenticate(tracker.conn, jwtResponse.Data.Token)
	}
	tracker.connMutex.Unlock()
}

// checkTimersTTL verifica se os timers expiraram (passo 4) e para servidores (passo 5)
func (sm *ServerMonitor) checkTimersTTL() {
	now := time.Now()

	for _, tracker := range sm.servers {
		tracker.mutex.RLock()
		hasTimer := tracker.HasTimer
		isOnline := tracker.IsOnline
		timerStart := tracker.InactivityTimerStart
		players := tracker.PlayersOnline
		tracker.mutex.RUnlock()

		// Verificar se timer expirou
		if hasTimer && isOnline && players == 0 {
			// Verificar se o timer foi iniciado (não é zero time)
			if !timerStart.IsZero() {
				elapsed := now.Sub(timerStart)
				if elapsed >= sm.inactivityTimeout {
					// Timer expirado - parar servidor (passo 5)
					fmt.Printf("[%s] Parando servidor (sem jogadores há %s)\n",
						tracker.ServerName, formatDuration(sm.inactivityTimeout))

					// Marcar timer como não ativo antes de parar
					tracker.mutex.Lock()
					tracker.HasTimer = false
					tracker.mutex.Unlock()

					go sm.stopServer(tracker)
				}
			}
		}
	}
}

// stopServer para um servidor (passo 5)
// Antes de parar, verifica quantos jogadores estão no servidor
func (sm *ServerMonitor) stopServer(tracker *ServerTracker) {
	tracker.connMutex.RLock()
	conn := tracker.conn
	tracker.connMutex.RUnlock()

	if conn == nil {
		// Tentar conectar temporariamente para parar
		var err error
		conn, err = ws.GetConnection(tracker.ServerID, tracker.ServerName)
		if err != nil {
			fmt.Printf("[%s] [ERRO] Falha ao conectar para parar servidor: %v\n", tracker.ServerName, err)
			return
		}
		defer conn.Close()
	}

	// ANTES DE PARAR: Verificar quantos jogadores estão no servidor
	// Enviar comando list para verificar jogadores
	if err := ws.SendCommand(conn, "list"); err == nil {
		// Aguardar resposta do comando list
		time.Sleep(2 * time.Second)

		// Verificar novamente a contagem de jogadores
		tracker.mutex.RLock()
		currentPlayers := tracker.PlayersOnline
		tracker.mutex.RUnlock()

		if currentPlayers > 0 {
			fmt.Printf("[%s] Parada cancelada (%d jogador(es) online)\n",
				tracker.ServerName, currentPlayers)
			// Cancelar timer já que há jogadores
			tracker.mutex.Lock()
			tracker.HasTimer = false
			tracker.InactivityTimerStart = time.Time{}
			tracker.mutex.Unlock()
			return
		}
	}

	// Parar o servidor
	ws.StopServer(conn, tracker.ServerName)

	// Atualizar estado
	tracker.mutex.Lock()
	tracker.HasTimer = false
	tracker.mutex.Unlock()
}

// closeServerConnection fecha a conexão de um servidor
func (sm *ServerMonitor) closeServerConnection(tracker *ServerTracker) {
	tracker.connMutex.Lock()
	defer tracker.connMutex.Unlock()

	if tracker.conn != nil {
		tracker.conn.Close()
		tracker.conn = nil
	}
}

// closeAllConnections fecha todas as conexões
func (sm *ServerMonitor) closeAllConnections() {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	for _, tracker := range sm.servers {
		sm.closeServerConnection(tracker)
		sm.stopFakeServer(tracker)
	}
}

// formatDuration formata duração de forma legível
func formatDuration(d time.Duration) string {
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	} else if minutes > 0 {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}

// startFakeServer inicia o servidor falso quando o servidor real está offline
// Esta função pode ser chamada múltiplas vezes - sempre garante que o fake server está rodando
func (sm *ServerMonitor) startFakeServer(tracker *ServerTracker) {
	// Verificar se o servidor está realmente offline antes de iniciar
	tracker.mutex.RLock()
	isOnline := tracker.IsOnline
	tracker.mutex.RUnlock()

	// Só iniciar fake server se o servidor estiver offline
	if isOnline {
		// Servidor está online - não iniciar fake server
		return
	}

	tracker.fakeServerMutex.Lock()
	defer tracker.fakeServerMutex.Unlock()

	// Se já existe um fake server rodando, verificar se precisa reiniciar
	if tracker.fakeServer != nil {
		// Verificar se a porta ainda está correta
		currentPort, err := api.GetServerPort(tracker.ServerID)
		if err == nil && currentPort == tracker.fakeServerPort {
			// Fake server já está rodando na porta correta - não fazer nada
			return
		}
		// Porta mudou ou erro - parar fake server antigo e iniciar novo
		tracker.fakeServer.Stop()
		tracker.fakeServer = nil
	}

	// Obter porta do servidor real
	port, err := api.GetServerPort(tracker.ServerID)
	if err != nil {
		fmt.Printf("[%s] [ERRO] Falha ao obter porta do servidor: %v\n", tracker.ServerName, err)
		return
	}

	tracker.fakeServerPort = port

	// Criar e iniciar fake server na mesma porta do servidor real
	// Usar a mensagem de login (será usada quando jogador tentar entrar)
	fakeServer := fakeserver.NewFakeServer(port, fakeserver.LoginMessage)

	if err := fakeServer.Start(); err != nil {
		fmt.Printf("[%s] [ERRO] Falha ao iniciar servidor falso na porta %d: %v\n", tracker.ServerName, port, err)
		return
	}

	tracker.fakeServer = fakeServer
	fmt.Printf("[%s] [INFO] Servidor falso iniciado na porta %d (servidor real está offline)\n", tracker.ServerName, port)

	// Monitorar conexões no fake server
	go sm.monitorFakeServerConnections(tracker)
}

// stopFakeServer para o servidor falso
func (sm *ServerMonitor) stopFakeServer(tracker *ServerTracker) {
	tracker.fakeServerMutex.Lock()
	defer tracker.fakeServerMutex.Unlock()

	if tracker.fakeServer != nil {
		tracker.fakeServer.Stop()
		tracker.fakeServer = nil
		fmt.Printf("[%s] [INFO] Servidor falso parado\n", tracker.ServerName)
	}
}

// ensureFakeServerRunning garante que o fake server está rodando apenas se o servidor estiver offline
// Esta função verifica e reinicia o fake server se necessário, mesmo que já tenha sido iniciado antes
func (sm *ServerMonitor) ensureFakeServerRunning(tracker *ServerTracker) {
	// Verificar se o servidor está realmente offline
	tracker.mutex.RLock()
	isOnline := tracker.IsOnline
	tracker.mutex.RUnlock()

	// Se o servidor está online, não iniciar fake server
	if isOnline {
		return
	}

	tracker.fakeServerMutex.Lock()
	hasFakeServer := tracker.fakeServer != nil
	tracker.fakeServerMutex.Unlock()

	if !hasFakeServer {
		// Não há fake server - iniciar um novo
		sm.startFakeServer(tracker)
	} else {
		// Já existe fake server - verificar se ainda está funcionando
		// Se o servidor ficou offline novamente, garantir que o fake server está ativo
		// (pode ter sido fechado após 60 segundos mas servidor ainda está offline)
		tracker.fakeServerMutex.RLock()
		fakeServer := tracker.fakeServer
		tracker.fakeServerMutex.RUnlock()

		// Se o fake server existe mas o servidor ainda está offline,
		// não precisamos fazer nada - o fake server já está rodando
		// Mas vamos garantir que está na porta correta
		if fakeServer != nil {
			// Verificar se a porta ainda está correta
			currentPort, err := api.GetServerPort(tracker.ServerID)
			if err == nil && currentPort != tracker.fakeServerPort {
				// Porta mudou - reiniciar fake server
				fmt.Printf("[%s] [INFO] Porta do servidor mudou, reiniciando fake server...\n", tracker.ServerName)
				sm.stopFakeServer(tracker)
				time.Sleep(100 * time.Millisecond)
				sm.startFakeServer(tracker)
			}
		}
	}
}

// monitorFakeServerConnections monitora conexões no fake server
// Quando detecta uma conexão, inicia o servidor real e aguarda 60 segundos antes de fechar o fake server
func (sm *ServerMonitor) monitorFakeServerConnections(tracker *ServerTracker) {
	// Aguardar por uma conexão (timeout de 5 minutos)
	tracker.fakeServerMutex.RLock()
	fakeServer := tracker.fakeServer
	tracker.fakeServerMutex.RUnlock()

	if fakeServer == nil {
		return
	}

	// Aguardar conexão (timeout de 5 minutos)
	hasConnection := fakeServer.WaitForConnection(5 * time.Minute)

	if !hasConnection {
		// Nenhuma conexão - não fazer nada, manter fake server rodando
		return
	}

	fmt.Printf("[%s] [INFO] Conexão detectada no servidor falso. Aguardando 500ms antes de iniciar servidor real...\n", tracker.ServerName)

	// Aguardar 500ms para dar tempo do usuário ver a mensagem
	time.Sleep(500 * time.Millisecond)

	// Iniciar servidor real
	if err := api.StartServer(tracker.ServerID); err != nil {
		fmt.Printf("[%s] [ERRO] Falha ao iniciar servidor real: %v\n", tracker.ServerName, err)
		return
	}

	fmt.Printf("[%s] [INFO] Servidor real iniciado. Aguardando 60 segundos antes de fechar servidor falso...\n", tracker.ServerName)

	// Aguardar 60 segundos
	time.Sleep(60 * time.Second)

	// Verificar se o servidor real está online antes de fechar o fake server
	tracker.mutex.RLock()
	isOnline := tracker.IsOnline
	tracker.mutex.RUnlock()

	// Fechar fake server apenas se o servidor real estiver online
	if isOnline {
		tracker.fakeServerMutex.Lock()
		if tracker.fakeServer != nil {
			tracker.fakeServer.Stop()
			tracker.fakeServer = nil
			fmt.Printf("[%s] [INFO] Servidor falso fechado após 60 segundos (servidor real está online)\n", tracker.ServerName)
		}
		tracker.fakeServerMutex.Unlock()
	} else {
		// Servidor ainda está offline - manter ou reiniciar fake server
		fmt.Printf("[%s] [INFO] Servidor real ainda está offline, mantendo fake server ativo\n", tracker.ServerName)
		// Garantir que o fake server está rodando
		sm.ensureFakeServerRunning(tracker)
	}
}
