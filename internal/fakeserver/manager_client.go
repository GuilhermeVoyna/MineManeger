package fakeserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

// ManagerClient faz chamadas HTTP para o manager
type ManagerClient struct {
	apiURL       string
	containerName string
	httpClient   *http.Client
}

// NewManagerClient cria um novo cliente para a API do manager
func NewManagerClient() *ManagerClient {
	apiURL := os.Getenv("MANAGER_API_URL")
	if apiURL == "" {
		apiURL = "http://host.docker.internal:8080" // Default
	}

	containerName := os.Getenv("CONTAINER_NAME")
	if containerName == "" {
		containerName = "unknown"
	}

	return &ManagerClient{
		apiURL:        apiURL,
		containerName: containerName,
		httpClient: &http.Client{
			Timeout: 10 * time.Second, // Aumentar timeout para 10 segundos
		},
	}
}

// RequestRemoveContainer solicita ao manager que remova este container
// FORÇA a notificação - tenta múltiplas vezes se necessário
func (mc *ManagerClient) RequestRemoveContainer() error {
	url := fmt.Sprintf("%s/remove-container", mc.apiURL)
	
	payload := map[string]string{
		"container_name": mc.containerName,
	}
	
	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[MANAGER CLIENT] [ERRO] Erro ao criar JSON: %v", err)
		return fmt.Errorf("erro ao criar JSON: %v", err)
	}

	log.Printf("[MANAGER CLIENT] ===== ENVIANDO REQUISIÇÃO PARA REMOVER CONTAINER =====")
	log.Printf("[MANAGER CLIENT] Container: %s", mc.containerName)
	log.Printf("[MANAGER CLIENT] URL: %s", url)
	log.Printf("[MANAGER CLIENT] Payload: %s", string(jsonData))

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("[MANAGER CLIENT] [ERRO] Erro ao criar requisição: %v", err)
		return fmt.Errorf("erro ao criar requisição: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := mc.httpClient.Do(req)
	if err != nil {
		log.Printf("[MANAGER CLIENT] [ERRO] Erro ao fazer requisição HTTP: %v", err)
		log.Printf("[MANAGER CLIENT] [ERRO] URL tentada: %s", url)
		log.Printf("[MANAGER CLIENT] [ERRO] Container name: %s", mc.containerName)
		return fmt.Errorf("erro ao fazer requisição: %v", err)
	}
	defer resp.Body.Close()

	// Ler resposta completa para debug
	bodyBytes := make([]byte, 4096)
	n, _ := resp.Body.Read(bodyBytes)
	responseBody := string(bodyBytes[:n])
	
	log.Printf("[MANAGER CLIENT] Resposta do manager:")
	log.Printf("[MANAGER CLIENT]   Status Code: %d", resp.StatusCode)
	log.Printf("[MANAGER CLIENT]   Body: %s", responseBody)

	if resp.StatusCode != http.StatusOK {
		log.Printf("[MANAGER CLIENT] [ERRO] Status code não OK: %d", resp.StatusCode)
		log.Printf("[MANAGER CLIENT] [ERRO] Response body: %s", responseBody)
		return fmt.Errorf("erro na resposta: status %d, body: %s", resp.StatusCode, responseBody)
	}

	log.Printf("[MANAGER CLIENT] ✓✓✓ CONTAINER %s REMOVIDO COM SUCESSO VIA API ✓✓✓", mc.containerName)
	return nil
}

// RequestShutdownContainer solicita ao manager que desligue este container
func (mc *ManagerClient) RequestShutdownContainer() error {
	url := fmt.Sprintf("%s/shutdown-container", mc.apiURL)
	
	payload := map[string]string{
		"container_name": mc.containerName,
	}
	
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("erro ao criar JSON: %v", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("erro ao criar requisição: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := mc.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("erro ao fazer requisição: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("erro na resposta: status %d", resp.StatusCode)
	}

	return nil
}

