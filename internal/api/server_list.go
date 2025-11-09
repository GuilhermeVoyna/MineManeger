package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type Server struct {
	ID          string `json:"identifier"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Status      string `json:"status"`
}

type ServerListResponse struct {
	Object string `json:"object"`
	Data   []struct {
		Object     string                 `json:"object"`
		Attributes map[string]interface{} `json:"attributes"`
	} `json:"data"`
}

// ListServers lista todos os servidores disponíveis
func ListServers() ([]Server, error) {
	userToken := "ptlc_dGSbeYIKJPi9SUUUvCeet2VIkyqMRpfLe39Qar3LW4r"
	domain := "https://painel.riguila.com.br"
	url := fmt.Sprintf("%s/api/client", domain)

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", userToken))
	req.Header.Add("Accept", "Application/vnd.pterodactyl.v1+json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status %d: %s", resp.StatusCode, resp.Status)
	}

	var result ServerListResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	var servers []Server
	for _, item := range result.Data {
		attrs := item.Attributes
		
		id, _ := attrs["identifier"].(string)
		name, _ := attrs["name"].(string)
		description, _ := attrs["description"].(string)
		
		servers = append(servers, Server{
			ID:          id,
			Name:        name,
			Description: description,
			Status:      "",
		})
	}

	return servers, nil
}

