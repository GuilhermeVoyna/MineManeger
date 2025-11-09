package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// GetServerStatus obtém o status atual do servidor via Client API
func GetServerStatus(serverID string) (string, error) {
	userToken := "ptlc_dGSbeYIKJPi9SUUUvCeet2VIkyqMRpfLe39Qar3LW4r"
	domain := "https://painel.riguila.com.br"
	url := fmt.Sprintf("%s/api/client/servers/%s", domain, serverID)

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", userToken))
	req.Header.Add("Accept", "Application/vnd.pterodactyl.v1+json")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode response: %v", err)
	}

	if attrs, ok := result["attributes"].(map[string]interface{}); ok {
		if state, ok := attrs["current_state"].(string); ok && state != "" {
			return state, nil
		}
		if state, ok := attrs["status"].(string); ok && state != "" {
			return state, nil
		}
	}

	return "", nil
}

