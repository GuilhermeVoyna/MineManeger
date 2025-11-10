package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	// managerAPI é a instância do servidor HTTP do manager
	managerAPI     *ManagerAPI
	managerAPIMutex sync.RWMutex
)

// ManagerAPI gerencia a API HTTP do manager
type ManagerAPI struct {
	port           int
	containerNames map[string]bool // Containers registrados para remoção
	processingContainers map[string]bool // Containers que estão sendo processados
	mutex          sync.RWMutex
	stopContainerFunc func(containerName string) error // Função para parar container
	onContainerRemovedFunc func(containerName string)  // Callback quando container é removido
}

// NewManagerAPI cria uma nova instância da API do manager
func NewManagerAPI(port int, stopContainerFunc func(containerName string) error, onContainerRemovedFunc func(containerName string)) *ManagerAPI {
	return &ManagerAPI{
		port:                   port,
		containerNames:         make(map[string]bool),
		processingContainers:   make(map[string]bool),
		stopContainerFunc:      stopContainerFunc,
		onContainerRemovedFunc: onContainerRemovedFunc,
	}
}

// Start inicia o servidor HTTP da API
func (ma *ManagerAPI) Start() error {
	http.HandleFunc("/remove-container", ma.handleRemoveContainer)
	http.HandleFunc("/shutdown-container", ma.handleShutdownContainer)
	http.HandleFunc("/stop-fake-server", ma.handleStopFakeServer)
	http.HandleFunc("/health", ma.handleHealth)

	// Escutar em todas as interfaces (0.0.0.0) para aceitar conexões de outros containers
	// Mas validar que o IP de origem é da rede interna (validação em validateInternalRequest)
	addr := fmt.Sprintf("0.0.0.0:%d", ma.port)
	log.Printf("[MANAGER API] Servidor HTTP iniciado em %s (apenas rede interna - validação de IP ativa)\n", addr)
	return http.ListenAndServe(addr, nil)
}

// RegisterContainer registra um container para poder ser removido via API
func (ma *ManagerAPI) RegisterContainer(containerName string) {
	ma.mutex.Lock()
	defer ma.mutex.Unlock()
	ma.containerNames[containerName] = true
}

// UnregisterContainer remove o registro de um container
func (ma *ManagerAPI) UnregisterContainer(containerName string) {
	ma.mutex.Lock()
	defer ma.mutex.Unlock()
	delete(ma.containerNames, containerName)
}

// handleRemoveContainer remove um container quando recebe requisição do fake server
func (ma *ManagerAPI) handleRemoveContainer(w http.ResponseWriter, r *http.Request) {
	// Validar que a requisição vem da rede interna
	if !validateInternalRequest(w, r) {
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ContainerName string `json:"container_name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	containerName := strings.TrimSpace(req.ContainerName)
	if containerName == "" {
		http.Error(w, "container_name is required", http.StatusBadRequest)
		return
	}

	// Verificar se o container está registrado
	ma.mutex.Lock()
	isRegistered := ma.containerNames[containerName]
	isProcessing := ma.processingContainers[containerName]
	
	if !isRegistered {
		ma.mutex.Unlock()
		http.Error(w, "Container not registered", http.StatusNotFound)
		return
	}

	// Se já está sendo processado, ignorar requisição duplicada
	if isProcessing {
		ma.mutex.Unlock()
		log.Printf("[MANAGER API] Container %s já está sendo processado, ignorando requisição duplicada\n", containerName)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "already_processing",
			"message": fmt.Sprintf("Container %s is already being processed", containerName),
		})
		return
	}

	// Marcar como sendo processado
	ma.processingContainers[containerName] = true
	ma.mutex.Unlock()

	log.Printf("[MANAGER API] ===== REMOÇÃO DE CONTAINER SOLICITADA =====")
	log.Printf("[MANAGER API] Container: %s\n", containerName)
	log.Printf("[MANAGER API] Callback disponível? %v\n", ma.onContainerRemovedFunc != nil)

	// Forçar remoção do container IMEDIATAMENTE (o fake server já aguardou 500ms)
	// Chamar callback de forma síncrona para garantir processamento
	if ma.onContainerRemovedFunc != nil {
		log.Printf("[MANAGER API] Chamando callback OnContainerRemoved para container %s (síncrono)\n", containerName)
		
		// Chamar callback em goroutine mas aguardar processamento
		done := make(chan bool, 1)
		go func() {
			defer func() {
				// Remover da lista de processamento após concluir
				ma.mutex.Lock()
				delete(ma.processingContainers, containerName)
				ma.mutex.Unlock()
				log.Printf("[MANAGER API] Callback concluído para container %s\n", containerName)
				done <- true
			}()
			
			// Chamar callback que remove container e inicia servidor real
			ma.onContainerRemovedFunc(containerName)
		}()
		
		// Aguardar até 10 segundos pelo processamento (timeout de segurança)
		select {
		case <-done:
			log.Printf("[MANAGER API] Processamento concluído para container %s\n", containerName)
		case <-time.After(10 * time.Second):
			log.Printf("[MANAGER API] [AVISO] Timeout ao processar remoção do container %s\n", containerName)
			ma.mutex.Lock()
			delete(ma.processingContainers, containerName)
			ma.mutex.Unlock()
		}
	} else {
		log.Printf("[MANAGER API] [ERRO] Callback OnContainerRemoved não está configurado!\n")
		// Se não há callback, pelo menos tentar parar o container
		if ma.stopContainerFunc != nil {
			log.Printf("[MANAGER API] Tentando parar container %s diretamente...\n", containerName)
			ma.stopContainerFunc(containerName)
		}
		ma.mutex.Lock()
		delete(ma.processingContainers, containerName)
		ma.mutex.Unlock()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": fmt.Sprintf("Container %s removed", containerName),
	})
}

// handleShutdownContainer desliga um container (usado quando status response é enviado)
func (ma *ManagerAPI) handleShutdownContainer(w http.ResponseWriter, r *http.Request) {
	// Validar que a requisição vem da rede interna
	if !validateInternalRequest(w, r) {
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ContainerName string `json:"container_name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	containerName := strings.TrimSpace(req.ContainerName)
	if containerName == "" {
		http.Error(w, "container_name is required", http.StatusBadRequest)
		return
	}

	// Verificar se o container está registrado
	ma.mutex.RLock()
	isRegistered := ma.containerNames[containerName]
	ma.mutex.RUnlock()

	if !isRegistered {
		http.Error(w, "Container not registered", http.StatusNotFound)
		return
	}

	// Parar o container
	if ma.stopContainerFunc != nil {
		if err := ma.stopContainerFunc(containerName); err != nil {
			http.Error(w, fmt.Sprintf("Error stopping container: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Remover do registro
	ma.UnregisterContainer(containerName)

	log.Printf("[MANAGER API] Container %s desligado via API (status response)\n", containerName)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": fmt.Sprintf("Container %s shutdown", containerName),
	})
}

// handleStopFakeServer para um fake server específico via API
func (ma *ManagerAPI) handleStopFakeServer(w http.ResponseWriter, r *http.Request) {
	// Validar que a requisição vem da rede interna
	if !validateInternalRequest(w, r) {
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ContainerName string `json:"container_name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	containerName := strings.TrimSpace(req.ContainerName)
	if containerName == "" {
		http.Error(w, "container_name is required", http.StatusBadRequest)
		return
	}

	// Verificar se o container está registrado
	ma.mutex.RLock()
	isRegistered := ma.containerNames[containerName]
	ma.mutex.RUnlock()

	if !isRegistered {
		http.Error(w, "Container not registered", http.StatusNotFound)
		return
	}

	// Parar o container usando a função de parada
	if ma.stopContainerFunc != nil {
		log.Printf("[MANAGER API] Parando fake server: %s\n", containerName)
		if err := ma.stopContainerFunc(containerName); err != nil {
			log.Printf("[MANAGER API] [ERRO] Erro ao parar container: %v\n", err)
			http.Error(w, fmt.Sprintf("Error stopping container: %v", err), http.StatusInternalServerError)
			return
		}
		log.Printf("[MANAGER API] ✓ Fake server %s parado com sucesso\n", containerName)
	} else {
		log.Printf("[MANAGER API] [ERRO] Função stopContainerFunc não está configurada\n")
		http.Error(w, "Stop function not configured", http.StatusInternalServerError)
		return
	}

	// Remover do registro
	ma.UnregisterContainer(containerName)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": fmt.Sprintf("Fake server %s stopped", containerName),
	})
}

// handleHealth endpoint de health check
func (ma *ManagerAPI) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
	})
}

// isInternalRequest verifica se a requisição vem da rede interna do Docker
// Aceita apenas localhost, IPv6 localhost e IPs privados (rede Docker)
func isInternalRequest(r *http.Request) bool {
	// Obter IP de origem
	ipStr := r.RemoteAddr
	
	// Se tiver porta, remover
	if idx := strings.LastIndex(ipStr, ":"); idx != -1 {
		ipStr = ipStr[:idx]
	}

	// Remover [ ] do IPv6 se presente
	ipStr = strings.Trim(ipStr, "[]")

	// Parsear IP
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}

	// Verificar se é localhost (IPv4 ou IPv6)
	if ip.IsLoopback() {
		return true
	}

	// Verificar se é IP privado (rede Docker)
	// Docker usa redes privadas: 172.16.0.0/12, 10.0.0.0/8, 192.168.0.0/16
	if ip.IsPrivate() {
		return true
	}

	// Verificar se é link-local (IPv6)
	if ip.IsLinkLocalUnicast() {
		return true
	}

	return false
}

// validateInternalRequest valida se a requisição vem da rede interna
// Retorna erro HTTP se não for interna
func validateInternalRequest(w http.ResponseWriter, r *http.Request) bool {
	if !isInternalRequest(r) {
		log.Printf("[MANAGER API] [SEGURANÇA] Requisição rejeitada de IP externo: %s\n", r.RemoteAddr)
		http.Error(w, "Forbidden: Only internal Docker network requests are allowed", http.StatusForbidden)
		return false
	}
	return true
}

// extractServerIDFromContainerName extrai o server ID do nome do container
// Formato esperado: fakeserver-<serverID>-<port>
func extractServerIDFromContainerName(containerName string) string {
	// Remover prefixo "fakeserver-"
	if !strings.HasPrefix(containerName, "fakeserver-") {
		return ""
	}

	parts := strings.Split(containerName, "-")
	if len(parts) < 3 {
		return ""
	}

	// Retornar o server ID (segunda parte)
	return parts[1]
}

// SetManagerAPI define a instância global da API do manager
func SetManagerAPI(api *ManagerAPI) {
	managerAPIMutex.Lock()
	defer managerAPIMutex.Unlock()
	managerAPI = api
}

// GetManagerAPI retorna a instância global da API do manager
func GetManagerAPI() *ManagerAPI {
	managerAPIMutex.RLock()
	defer managerAPIMutex.RUnlock()
	return managerAPI
}

