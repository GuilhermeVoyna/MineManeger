package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// StartServer inicia o servidor via API do Pterodactyl
func StartServer(serverID string) error {
	userToken := "ptlc_dGSbeYIKJPi9SUUUvCeet2VIkyqMRpfLe39Qar3LW4r"
	domain := "https://painel.riguila.com.br"
	url := fmt.Sprintf("%s/api/client/servers/%s/power", domain, serverID)

	// Criar payload para iniciar o servidor
	payload := map[string]string{
		"signal": "start",
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %v", err)
	}

	client := &http.Client{}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", userToken))
	req.Header.Add("Accept", "Application/vnd.pterodactyl.v1+json")
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	return nil
}

// GetServerPort obtém a porta do servidor
func GetServerPort(serverID string) (int, error) {
	userToken := "ptlc_dGSbeYIKJPi9SUUUvCeet2VIkyqMRpfLe39Qar3LW4r"
	domain := "https://painel.riguila.com.br"
	url := fmt.Sprintf("%s/api/client/servers/%s", domain, serverID)

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", userToken))
	req.Header.Add("Accept", "Application/vnd.pterodactyl.v1+json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %v", err)
	}

	// Tentar extrair porta de diferentes locais possíveis
	if attrs, ok := result["attributes"].(map[string]interface{}); ok {
		// Tentar allocations (array de allocations)
		if allocations, ok := attrs["allocations"].([]interface{}); ok && len(allocations) > 0 {
			if alloc, ok := allocations[0].(map[string]interface{}); ok {
				if port, ok := alloc["port"].(float64); ok {
					return int(port), nil
				}
			}
		}
		// Tentar port diretamente
		if port, ok := attrs["port"].(float64); ok {
			return int(port), nil
		}
	}

	return 25565, nil // Porta padrão do Minecraft
}

