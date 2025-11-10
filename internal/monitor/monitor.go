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
	"maneger/internal/docker"
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

	// Docker container manager
	containerManager *docker.ContainerManager

	// Controle
	stopChan chan struct{}

	// Mapeamento de container name para server ID
	containerToServerID map[string]string
	containerMutex      sync.RWMutex
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

	// Fake server container
	fakeServerContainerName string
	fakeServerPort          int
	fakeServerLoginChan     chan bool // Canal para receber notificações de login do container
	fakeServerStarting      bool      // Flag para evitar múltiplas tentativas de iniciar
	fakeServerStopping      bool      // Flag para evitar múltiplas tentativas de parar
	fakeServerRemoving      bool      // Flag para evitar múltiplas tentativas de remover/iniciar servidor
	fakeServerMutex         sync.RWMutex

	// Mutex para thread-safety
	mutex sync.RWMutex
}

// NewServerMonitor cria um novo monitor de servidores
func NewServerMonitor(checkInterval, inactivityTimeout time.Duration) *ServerMonitor {
	return &ServerMonitor{
		checkInterval:       checkInterval,
		inactivityTimeout:   inactivityTimeout,
		servers:             make(map[string]*ServerTracker),
		containerManager:    docker.NewContainerManager("mine-manager-fakeserver:latest"),
		stopChan:            make(chan struct{}),
		containerToServerID: make(map[string]string),
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

// Stop para o monitoramento e limpa todos os recursos
func (sm *ServerMonitor) Stop() {
	fmt.Println("[MONITOR] Parando monitor e limpando recursos...")

	// Parar todos os containers de fake server
	sm.containerManager.StopAllFakeServerContainers()

	// Fechar todas as conexões
	sm.closeAllConnections()

	// Fechar canal de parada
	close(sm.stopChan)

	fmt.Println("[MONITOR] Monitor parado")
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
			// Servidor está offline - SEMPRE garantir que fake server está rodando
			// ensureFakeServerRunning verifica se está rodando e inicia se necessário
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

		// Verificar se fake server está rodando
		tracker.fakeServerMutex.RLock()
		fakeServerContainerName := tracker.fakeServerContainerName
		tracker.fakeServerMutex.RUnlock()

		hasFakeServer := false
		if fakeServerContainerName != "" {
			hasFakeServer = sm.containerManager.IsFakeServerContainerRunning(fakeServerContainerName)
		}

		status := "offline"
		if isOnline {
			status = "online"
		}

		// Se servidor está offline e fake server está rodando, adicionar indicação
		if !isOnline && hasFakeServer {
			// Mostrar status do WebSocket se disponível
			if wsStatus != "" {
				status = fmt.Sprintf("%s (%s, fakeing)", status, wsStatus)
			} else {
				status = fmt.Sprintf("%s (fakeing)", status)
			}
		} else {
			// Mostrar status do WebSocket se disponível
			if wsStatus != "" {
				status = fmt.Sprintf("%s (%s)", status, wsStatus)
			} else if !isOnline {
				// Se ainda não recebemos status do WebSocket e está offline, mostrar "aguardando"
				status = "aguardando status..."
			}
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

	for range ticker.C {
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

// startFakeServer inicia o container do fake server quando o servidor real está offline
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

	// Verificar se já está iniciando ou parando
	if tracker.fakeServerStarting || tracker.fakeServerStopping {
		tracker.fakeServerMutex.Unlock()
		return
	}

	// Verificar se já existe um container rodando na porta correta
	if tracker.fakeServerContainerName != "" {
		if sm.containerManager.IsFakeServerContainerRunning(tracker.fakeServerContainerName) {
			// Verificar se a porta ainda está correta
			port, err := api.GetServerPort(tracker.ServerID)
			if err == nil && port > 0 && tracker.fakeServerPort == port {
				// Container já está rodando na porta correta - não fazer nada
				tracker.fakeServerMutex.Unlock()
				return
			}
		}
	}

	// Marcar como iniciando
	tracker.fakeServerStarting = true
	tracker.fakeServerMutex.Unlock()

	// Obter porta do servidor real
	port, err := api.GetServerPort(tracker.ServerID)
	if err != nil {
		tracker.fakeServerMutex.Lock()
		tracker.fakeServerStarting = false
		tracker.fakeServerMutex.Unlock()
		fmt.Printf("[%s] [ERRO] Falha ao obter porta do servidor: %v\n", tracker.ServerName, err)
		fmt.Printf("[%s] [INFO] Tentando novamente na próxima verificação...\n", tracker.ServerName)
		return
	}
	
	if port <= 0 || port >= 65536 {
		tracker.fakeServerMutex.Lock()
		tracker.fakeServerStarting = false
		tracker.fakeServerMutex.Unlock()
		fmt.Printf("[%s] [ERRO] Porta inválida obtida: %d\n", tracker.ServerName, port)
		return
	}
	
	fmt.Printf("[%s] [INFO] Porta obtida do Pterodactyl: %d\n", tracker.ServerName, port)

	// Verificar novamente se o servidor está offline (pode ter mudado)
	tracker.mutex.RLock()
	isOnline = tracker.IsOnline
	tracker.mutex.RUnlock()

	if isOnline {
		tracker.fakeServerMutex.Lock()
		tracker.fakeServerStarting = false
		tracker.fakeServerMutex.Unlock()
		return
	}

	tracker.fakeServerMutex.Lock()

	// Se já existe um container, parar primeiro
	if tracker.fakeServerContainerName != "" && tracker.fakeServerContainerName != fmt.Sprintf("fakeserver-%s-%d", tracker.ServerID, port) {
		oldContainerName := tracker.fakeServerContainerName
		tracker.fakeServerMutex.Unlock()

		// Parar container antigo (sem usar stopFakeServer para evitar deadlock)
		sm.containerManager.StopFakeServerContainer(oldContainerName)
		time.Sleep(200 * time.Millisecond)

		tracker.fakeServerMutex.Lock()
	}

	tracker.fakeServerPort = port
	containerName := fmt.Sprintf("fakeserver-%s-%d", tracker.ServerID, port)

	// Criar canal para receber notificações de login se ainda não existe
	if tracker.fakeServerLoginChan == nil {
		tracker.fakeServerLoginChan = make(chan bool, 1)
	}

	tracker.fakeServerMutex.Unlock()

	// Iniciar container do fake server
	if err := sm.containerManager.StartFakeServerContainer(
		containerName,
		port,
		fakeserver.StatusMessage,
		fakeserver.LoginMessage,
	); err != nil {
		tracker.fakeServerMutex.Lock()
		tracker.fakeServerStarting = false
		tracker.fakeServerMutex.Unlock()
		
		// Verificar se o erro é porque a porta já está em uso
		if strings.Contains(err.Error(), "já está em uso") {
			fmt.Printf("[%s] [AVISO] Porta %d já está em uso por outro container. Verificando se é de outro servidor...\n", tracker.ServerName, port)
			// Tentar encontrar qual container está usando a porta
			existingContainer := sm.containerManager.FindContainerByPort(port)
			if existingContainer != "" {
				// Verificar se o container pertence a outro servidor
				sm.containerMutex.RLock()
				otherServerID, exists := sm.containerToServerID[existingContainer]
				sm.containerMutex.RUnlock()
				
				if exists && otherServerID != tracker.ServerID {
					// Obter nome do outro servidor
					sm.mutex.RLock()
					otherTracker, otherExists := sm.servers[otherServerID]
					otherServerName := otherServerID
					if otherExists {
						otherServerName = otherTracker.ServerName
					}
					sm.mutex.RUnlock()
					
					fmt.Printf("[%s] [AVISO] Porta %d está sendo usada pelo servidor '%s' (container: %s). "+
						"Isso pode indicar que ambos os servidores têm a mesma porta no Pterodactyl.\n",
						tracker.ServerName, port, otherServerName, existingContainer)
					fmt.Printf("[%s] [INFO] Fake server não será criado para este servidor enquanto a porta estiver ocupada.\n", tracker.ServerName)
				} else {
					fmt.Printf("[%s] [AVISO] Porta %d está em uso (container: %s). Tentando novamente na próxima verificação.\n",
						tracker.ServerName, port, existingContainer)
				}
			}
			// Não tratar como erro fatal - apenas logar e continuar
			return
		}
		
		fmt.Printf("[%s] [ERRO] Falha ao iniciar container do fake server na porta %d: %v\n", tracker.ServerName, port, err)
		return
	}

	tracker.fakeServerMutex.Lock()
	tracker.fakeServerContainerName = containerName
	tracker.fakeServerStarting = false
	tracker.fakeServerMutex.Unlock()

	fmt.Printf("[%s] [INFO] Container do fake server iniciado na porta %d (servidor real está offline)\n", tracker.ServerName, port)

	// Registrar mapeamento container -> server ID
	sm.containerMutex.Lock()
	sm.containerToServerID[containerName] = tracker.ServerID
	sm.containerMutex.Unlock()

	// Registrar container na API do manager
	if managerAPI := api.GetManagerAPI(); managerAPI != nil {
		managerAPI.RegisterContainer(containerName)
		fmt.Printf("[%s] [INFO] Container registrado na API do manager: %s\n", tracker.ServerName, containerName)
	}

	// NÃO monitorar logs mais - o fake server se comunica diretamente via API
	// Remover monitoramento de logs (não é mais necessário)
	// O fake server chama a API diretamente quando detecta login
}

// stopFakeServer para o container do fake server
func (sm *ServerMonitor) stopFakeServer(tracker *ServerTracker) {
	tracker.fakeServerMutex.Lock()

	// Verificar se já está parando
	if tracker.fakeServerStopping {
		tracker.fakeServerMutex.Unlock()
		return
	}

	// Se não há container, não fazer nada
	if tracker.fakeServerContainerName == "" {
		tracker.fakeServerMutex.Unlock()
		return
	}

	containerName := tracker.fakeServerContainerName

	// Marcar como parando
	tracker.fakeServerStopping = true
	tracker.fakeServerMutex.Unlock()

	// Remover mapeamento container -> server ID
	sm.containerMutex.Lock()
	delete(sm.containerToServerID, containerName)
	sm.containerMutex.Unlock()

	// Remover registro da API do manager
	if managerAPI := api.GetManagerAPI(); managerAPI != nil {
		managerAPI.UnregisterContainer(containerName)
	}

	if err := sm.containerManager.StopFakeServerContainer(containerName); err != nil {
		fmt.Printf("[%s] [ERRO] Erro ao parar container do fake server: %v\n", tracker.ServerName, err)
	} else {
		fmt.Printf("[%s] [INFO] Container do fake server parado\n", tracker.ServerName)
	}

	tracker.fakeServerMutex.Lock()
	tracker.fakeServerContainerName = ""
	tracker.fakeServerLoginChan = nil
	tracker.fakeServerStopping = false
	tracker.fakeServerMutex.Unlock()
}

// ensureFakeServerRunning garante que o container do fake server está rodando apenas se o servidor estiver offline
// Esta função verifica e reinicia o fake server se necessário, mesmo que já tenha sido iniciado antes
// IMPORTANTE: Esta função deve ser chamada sempre que o servidor estiver offline para garantir que o fake server está rodando
func (sm *ServerMonitor) ensureFakeServerRunning(tracker *ServerTracker) {
	// Verificar se o servidor está realmente offline
	tracker.mutex.RLock()
	isOnline := tracker.IsOnline
	tracker.mutex.RUnlock()

	// Se o servidor está online, não iniciar fake server
	if isOnline {
		// Servidor está online - garantir que fake server está parado
		sm.stopFakeServer(tracker)
		return
	}

	// Servidor está offline - verificar se fake server está rodando
	tracker.fakeServerMutex.RLock()
	containerName := tracker.fakeServerContainerName
	isStarting := tracker.fakeServerStarting
	isStopping := tracker.fakeServerStopping
	tracker.fakeServerMutex.RUnlock()

	// Se já está iniciando ou parando, não fazer nada
	if isStarting || isStopping {
		return
	}

	// Se não há container ou não está rodando, iniciar um novo
	if containerName == "" || !sm.containerManager.IsFakeServerContainerRunning(containerName) {
		fmt.Printf("[%s] [INFO] Servidor está offline e fake server não está rodando. Iniciando fake server...\n", tracker.ServerName)
		sm.startFakeServer(tracker)
		return
	}

	// Container existe - verificar se a porta ainda está correta
	currentPort, err := api.GetServerPort(tracker.ServerID)
	if err != nil {
		fmt.Printf("[%s] [AVISO] Não foi possível verificar se a porta mudou: %v\n", tracker.ServerName, err)
		// Continuar - o container pode estar na porta correta
	} else if currentPort > 0 && currentPort != tracker.fakeServerPort {
		// Porta mudou - reiniciar container
		fmt.Printf("[%s] [INFO] Porta do servidor mudou de %d para %d, reiniciando container do fake server...\n", tracker.ServerName, tracker.fakeServerPort, currentPort)
		sm.stopFakeServer(tracker)
		time.Sleep(200 * time.Millisecond)
		sm.startFakeServer(tracker)
		return
	}

	// Container está rodando e porta está correta - tudo OK
	// Mas garantir que está registrado na API do manager
	if managerAPI := api.GetManagerAPI(); managerAPI != nil {
		managerAPI.RegisterContainer(containerName)
	}
}

// startServerViaWebSocket inicia o servidor via WebSocket usando "set state" com "start"
// GARANTE que a conexão está autenticada antes de enviar o comando
func (sm *ServerMonitor) startServerViaWebSocket(tracker *ServerTracker) error {
	tracker.connMutex.RLock()
	conn := tracker.conn
	tracker.connMutex.RUnlock()

	// Se não há conexão, criar uma nova e autenticar
	// SEMPRE garantir autenticação antes de enviar comando crítico
	if conn == nil {
		fmt.Printf("[%s] [INFO] Nenhuma conexão WebSocket ativa. Criando nova conexão...\n", tracker.ServerName)
		
		// Obter JWT primeiro
		jwtResponse, err := api.GetJwt(tracker.ServerID)
		if err != nil {
			return fmt.Errorf("falha ao obter JWT: %v", err)
		}

		// Conectar ao WebSocket
		var errConn error
		conn, errConn = ws.GetConnection(tracker.ServerID, tracker.ServerName)
		if errConn != nil {
			return fmt.Errorf("falha ao conectar WebSocket: %v", errConn)
		}

		// AUTENTICAR a conexão via mensagem "auth" (obrigatório após conectar)
		fmt.Printf("[%s] [INFO] Autenticando conexão WebSocket com token JWT...\n", tracker.ServerName)
		if err := ws.Authenticate(conn, jwtResponse.Data.Token); err != nil {
			conn.Close()
			return fmt.Errorf("falha ao autenticar WebSocket: %v", err)
		}

		fmt.Printf("[%s] [INFO] ✓ Conexão WebSocket autenticada com sucesso\n", tracker.ServerName)
		
		// Aguardar para garantir que a autenticação foi processada pelo servidor
		fmt.Printf("[%s] [INFO] Aguardando processamento de autenticação (1 segundo)...\n", tracker.ServerName)
		time.Sleep(1 * time.Second)

		// Salvar a conexão no tracker para reutilização
		tracker.connMutex.Lock()
		tracker.conn = conn
		tracker.connMutex.Unlock()

		// Iniciar goroutine para ler mensagens da nova conexão
		go sm.readWebSocketMessages(tracker)
	} else {
		// Conexão existe - garantir que está autenticada antes de enviar comando crítico
		fmt.Printf("[%s] [INFO] Conexão WebSocket existente. Garantindo autenticação válida...\n", tracker.ServerName)
		
		// Obter novo JWT (tokens podem expirar) e reautenticar
		jwtResponse, err := api.GetJwt(tracker.ServerID)
		if err != nil {
			fmt.Printf("[%s] [AVISO] Falha ao obter JWT, usando conexão existente: %v\n", tracker.ServerName, err)
		} else {
			// Reautenticar para garantir que token está válido
			fmt.Printf("[%s] [INFO] Reautenticando conexão WebSocket...\n", tracker.ServerName)
			if err := ws.Authenticate(conn, jwtResponse.Data.Token); err != nil {
				fmt.Printf("[%s] [AVISO] Falha ao reautenticar, tentando continuar: %v\n", tracker.ServerName, err)
			} else {
				fmt.Printf("[%s] [INFO] ✓ Reautenticação bem-sucedida\n", tracker.ServerName)
				time.Sleep(500 * time.Millisecond)
			}
		}
	}

	// Enviar comando "set state" com "start" via WebSocket
	fmt.Printf("[%s] [INFO] Enviando comando 'set state start' via WebSocket...\n", tracker.ServerName)
	if err := ws.StartServer(conn); err != nil {
		return fmt.Errorf("falha ao enviar comando start via WebSocket: %v", err)
	}

	fmt.Printf("[%s] [INFO] ✓ Comando start enviado via WebSocket com sucesso\n", tracker.ServerName)
	return nil
}

// OnContainerRemoved é chamado quando a API remove um container
// Inicia o servidor real via WebSocket
func (sm *ServerMonitor) OnContainerRemoved(containerName string) {
	fmt.Printf("[MONITOR] [INFO] Callback OnContainerRemoved chamado para container: %s\n", containerName)
	
	// Obter server ID do container
	sm.containerMutex.RLock()
	serverID, exists := sm.containerToServerID[containerName]
	sm.containerMutex.RUnlock()

	if !exists {
		fmt.Printf("[MONITOR] [AVISO] Container %s não encontrado no mapeamento. Containers registrados: %v\n", containerName, sm.containerToServerID)
		return
	}

	fmt.Printf("[MONITOR] [INFO] Container %s mapeado para servidor ID: %s\n", containerName, serverID)

	// Obter tracker do servidor
	sm.mutex.RLock()
	tracker, exists := sm.servers[serverID]
	sm.mutex.RUnlock()

	if !exists {
		fmt.Printf("[MONITOR] [AVISO] Servidor %s não encontrado nos servidores monitorados\n", serverID)
		return
	}

	fmt.Printf("[MONITOR] [INFO] Servidor encontrado: %s (ID: %s)\n", tracker.ServerName, serverID)

	// Verificar se já está processando remoção
	tracker.fakeServerMutex.Lock()
	if tracker.fakeServerRemoving {
		tracker.fakeServerMutex.Unlock()
		fmt.Printf("[%s] [INFO] Remoção já está sendo processada, ignorando requisição duplicada\n", tracker.ServerName)
		return
	}
	tracker.fakeServerRemoving = true
	tracker.fakeServerMutex.Unlock()

	fmt.Printf("[%s] [INFO] ===== INICIANDO PROCESSO DE REMOÇÃO E INÍCIO DO SERVIDOR REAL =====\n", tracker.ServerName)
	fmt.Printf("[%s] [INFO] Requisição de remoção recebida via API\n", tracker.ServerName)

	// REMOVER CONTAINER IMEDIATAMENTE (fake server já aguardou 500ms)
	fmt.Printf("[%s] [INFO] FORÇANDO remoção do container do fake server...\n", tracker.ServerName)
	if err := sm.containerManager.StopFakeServerContainer(containerName); err != nil {
		fmt.Printf("[%s] [ERRO] Erro ao parar container: %v\n", tracker.ServerName, err)
	} else {
		fmt.Printf("[%s] [INFO] ✓ Container %s removido com sucesso\n", tracker.ServerName, containerName)
	}
	
	// Atualizar estado do tracker
	tracker.fakeServerMutex.Lock()
	tracker.fakeServerContainerName = ""
	tracker.fakeServerLoginChan = nil
	tracker.fakeServerMutex.Unlock()

	// Remover do registro da API do manager
	if managerAPI := api.GetManagerAPI(); managerAPI != nil {
		managerAPI.UnregisterContainer(containerName)
		fmt.Printf("[%s] [INFO] Container removido do registro da API do manager\n", tracker.ServerName)
	}

	// Remover do mapeamento
	sm.containerMutex.Lock()
	delete(sm.containerToServerID, containerName)
	sm.containerMutex.Unlock()

	// NÃO aguardar - iniciar servidor real IMEDIATAMENTE
	fmt.Printf("[%s] [INFO] Container removido. Iniciando servidor real IMEDIATAMENTE...\n", tracker.ServerName)

	// Iniciar servidor via WebSocket
	fmt.Printf("[%s] [INFO] Iniciando servidor real via WebSocket...\n", tracker.ServerName)
	if err := sm.startServerViaWebSocket(tracker); err != nil {
		fmt.Printf("[%s] [ERRO] Falha ao iniciar servidor real via WebSocket: %v\n", tracker.ServerName, err)
		// Se falhar, recriar fake server
		time.Sleep(1 * time.Second)
		fmt.Printf("[%s] [INFO] Recriando fake server devido ao erro...\n", tracker.ServerName)
		sm.ensureFakeServerRunning(tracker)
		
		// Resetar flag de remoção
		tracker.fakeServerMutex.Lock()
		tracker.fakeServerRemoving = false
		tracker.fakeServerMutex.Unlock()
		return
	}

	fmt.Printf("[%s] [INFO] Comando start enviado via WebSocket com sucesso\n", tracker.ServerName)

	// Aguardar 60 segundos e verificar se subiu
	go func() {
		fmt.Printf("[%s] [INFO] Servidor real iniciado via WebSocket. Aguardando 60 segundos para verificar se subiu com sucesso...\n", tracker.ServerName)
		time.Sleep(60 * time.Second)

		// Verificar se o servidor foi subido com sucesso
		tracker.mutex.RLock()
		isOnline := tracker.IsOnline
		tracker.mutex.RUnlock()

		// Resetar flag de remoção
		tracker.fakeServerMutex.Lock()
		tracker.fakeServerRemoving = false
		tracker.fakeServerMutex.Unlock()

		fmt.Printf("[%s] [INFO] Verificação após 60 segundos: servidor está online? %v\n", tracker.ServerName, isOnline)

		if isOnline {
			fmt.Printf("[%s] [INFO] ✓ Servidor real está online e funcionando corretamente\n", tracker.ServerName)
		} else {
			// Servidor não subiu com sucesso - recriar fake server
			fmt.Printf("[%s] [AVISO] ✗ Servidor real não está online após 60 segundos. Recriando container do fake server...\n", tracker.ServerName)
			sm.ensureFakeServerRunning(tracker)
		}
	}()
}

// monitorFakeServerConnections foi removido - não é mais necessário
// O fake server se comunica diretamente com a API do manager quando detecta login
