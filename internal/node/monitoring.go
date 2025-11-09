package node

import (
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

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

// MonitoringState gerencia o estado do monitoramento
type MonitoringState struct {
	PlayersOnline      int
	LastPlayerActivity time.Time
	ServerStopped      bool
	InactivityTimer    *time.Timer
	Mutex              *sync.Mutex
	InactivityTimeout  time.Duration
	FirstListReceived  bool // Flag para saber se já recebeu primeira resposta do list
	ServerID           string // ID do servidor para reconexão
	ServerName         string // Nome do servidor para logs
}

// NewMonitoringState cria um novo estado de monitoramento
func NewMonitoringState(timeout time.Duration, serverID string, serverName string) *MonitoringState {
	timer := time.NewTimer(timeout)
	timer.Stop()
	return &MonitoringState{
		PlayersOnline:      0,
		LastPlayerActivity: time.Now(),
		ServerStopped:      false,
		InactivityTimer:    timer,
		Mutex:              &sync.Mutex{},
		InactivityTimeout:  timeout,
		FirstListReceived:  false,
		ServerID:           serverID,
		ServerName:         serverName,
	}
}

// updatePlayerCountFromList atualiza contador baseado no comando "list"
func (m *MonitoringState) updatePlayerCountFromList(count int) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	oldCount := m.PlayersOnline
	m.PlayersOnline = count
	m.LastPlayerActivity = time.Now()
	isFirstList := !m.FirstListReceived
	m.FirstListReceived = true

	if m.PlayersOnline > 0 {
		// PARAR o timer quando há jogadores online
		m.InactivityTimer.Stop()
		if oldCount != count || isFirstList {
			fmt.Printf("[%s] [INFO] Contagem atualizada via 'list': %d jogadores online - Timer SUSPENSO\n", m.ServerName, count)
		}
	} else {
		// Reiniciar timer quando não há jogadores
		m.InactivityTimer.Reset(m.InactivityTimeout)
		timeoutStr := formatDuration(m.InactivityTimeout)
		// Sempre mostrar mensagem na primeira resposta do list ou quando count mudar
		if oldCount != count || isFirstList {
			if isFirstList {
				fmt.Printf("[%s] [INFO] Contagem confirmada via 'list': 0 jogadores online - Timer ATIVO (%s)\n", m.ServerName, timeoutStr)
			} else {
				fmt.Printf("[%s] [INFO] Contagem atualizada via 'list': 0 jogadores online - Timer REINICIADO (%s)\n", m.ServerName, timeoutStr)
			}
		}
	}
}

// startInactivityMonitoring inicia a goroutine de monitoramento de inatividade
func (m *MonitoringState) startInactivityMonitoring(conn *websocket.Conn, stopServerFunc func()) {
	go func() {
		for range m.InactivityTimer.C {
			m.Mutex.Lock()
			timeSinceActivity := time.Since(m.LastPlayerActivity)
			currentPlayers := m.PlayersOnline
			m.Mutex.Unlock()

			// Só parar o servidor se não houver jogadores online
			if currentPlayers == 0 && timeSinceActivity >= m.InactivityTimeout {
				stopServerFunc()
				return
			} else if currentPlayers == 0 {
				// Resetar timer para o tempo restante apenas se não houver jogadores
				m.InactivityTimer.Reset(m.InactivityTimeout - timeSinceActivity)
			}
			// Se houver jogadores, o timer não será resetado (fica suspenso)
		}
	}()
}

// resetMonitoring reseta o estado de monitoramento
func (m *MonitoringState) resetMonitoring() {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	m.PlayersOnline = 0
	m.LastPlayerActivity = time.Now()
	m.ServerStopped = false
	m.FirstListReceived = false // Resetar flag para mostrar mensagem na próxima resposta do list
	m.InactivityTimer.Stop()
	m.InactivityTimer.Reset(m.InactivityTimeout)
}

// processPlayerActivity processa atividade de jogador nos logs
func (m *MonitoringState) processPlayerActivity(conn *websocket.Conn, logLine string) {
	// Primeiro, verificar se é resposta do comando "list"
	if count, ok := parseListCommand(logLine); ok {
		m.updatePlayerCountFromList(count)
		return
	}

	// Depois, verificar conexões/desconexões e solicitar "list" para confirmar
	if checkPlayerJoin(logLine) {
		// Solicitar "list" para obter contagem real
		time.Sleep(500 * time.Millisecond)
		sendCommand(conn, "list")
	} else if checkPlayerLeave(logLine) {
		// Solicitar "list" para obter contagem real
		time.Sleep(500 * time.Millisecond)
		sendCommand(conn, "list")
	}
}
