package node

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type WebSocketResponse struct {
	Data struct {
		Token  string `json:"token"`
		Socket string `json:"socket"`
	} `json:"data"`
}

// GetJwt obtém token JWT e URL do WebSocket para um servidor específico
func GetJwt(serverId string) (WebSocketResponse, error) {
	userToken := "ptlc_dGSbeYIKJPi9SUUUvCeet2VIkyqMRpfLe39Qar3LW4r"
	domain := "https://painel.riguila.com.br"
	url := fmt.Sprintf("%s/api/client/servers/%s/websocket", domain, serverId)

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return WebSocketResponse{}, fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", userToken))
	req.Header.Add("Accept", "Application/vnd.pterodactyl.v1+json")

	resp, err := client.Do(req)
	if err != nil {
		return WebSocketResponse{}, fmt.Errorf("failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return WebSocketResponse{}, fmt.Errorf("API returned status %d: %s", resp.StatusCode, resp.Status)
	}

	var result WebSocketResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return WebSocketResponse{}, fmt.Errorf("failed to decode response: %v", err)
	}

	return result, nil
}

