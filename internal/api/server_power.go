package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"maneger/internal/config"
)

// StartServer inicia o servidor via API do Pterodactyl
func StartServer(serverID string) error {
	userToken := config.GetUserToken()
	domain := config.GetDomain()
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
	userToken := config.GetUserToken()
	domain := config.GetDomain()
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
		// 1. PRIMEIRO: Tentar allocations em relationships.allocations.data (estrutura correta do Pterodactyl)
		if relationships, ok := attrs["relationships"].(map[string]interface{}); ok {
			if allocationsRel, ok := relationships["allocations"].(map[string]interface{}); ok {
				if allocationsData, ok := allocationsRel["data"].([]interface{}); ok && len(allocationsData) > 0 {
					// Usar a primeira allocation disponível (todas são válidas, não há allocation padrão)
					// O IP e porta estão na mesma resposta da API
					for _, allocInterface := range allocationsData {
						if alloc, ok := allocInterface.(map[string]interface{}); ok {
							// Verificar se tem attributes
							if allocAttrs, ok := alloc["attributes"].(map[string]interface{}); ok {
								// Extrair porta e IP da allocation
								if port, ok := allocAttrs["port"].(float64); ok {
									ip := ""
									if ipVal, ok := allocAttrs["ip"].(string); ok {
										ip = ipVal
									}
									
									// Construir endereço completo (IP:PORTA)
									address := ""
									if ip != "" {
										address = fmt.Sprintf("%s:%d", ip, int(port))
									}
									
									if address != "" {
										fmt.Printf("[API] Porta %d extraída da allocation (Endereço: %s)\n", int(port), address)
									} else {
										fmt.Printf("[API] Porta %d extraída da allocation\n", int(port))
									}
									return int(port), nil
								}
							}
						}
						// Usar apenas a primeira allocation encontrada
						break
					}
				}
			}
		}
		
		// 2. Tentar allocations diretamente em attributes (fallback para formato antigo)
		if allocations, ok := attrs["allocations"].([]interface{}); ok && len(allocations) > 0 {
			// Procurar pela allocation principal (is_default = true) ou a primeira
			for _, allocInterface := range allocations {
				if alloc, ok := allocInterface.(map[string]interface{}); ok {
					// Verificar se é a allocation padrão
					isDefault := false
					if def, ok := alloc["is_default"].(bool); ok && def {
						isDefault = true
					}
					
					// Extrair porta da allocation
					if port, ok := alloc["port"].(float64); ok {
						if isDefault {
							fmt.Printf("[API] Porta %d extraída da allocation padrão (formato direto)\n", int(port))
							return int(port), nil
						}
						// Se não encontramos a padrão ainda, guardar esta
						if !isDefault && len(allocations) == 1 {
							fmt.Printf("[API] Porta %d extraída da allocation (única, formato direto)\n", int(port))
							return int(port), nil
						}
					}
				}
			}
			
			// Se não encontramos a padrão, usar a primeira allocation
			if alloc, ok := allocations[0].(map[string]interface{}); ok {
				if port, ok := alloc["port"].(float64); ok {
					fmt.Printf("[API] Porta %d extraída da primeira allocation (formato direto)\n", int(port))
					return int(port), nil
				}
			}
		}
		
		// 3. Tentar extrair do campo "address" ou "Address" (formato: "IP:PORTA" ou "host:port")
		addressFields := []string{"address", "Address", "ADDRESS", "server_address"}
		for _, field := range addressFields {
			if address, ok := attrs[field].(string); ok && address != "" {
				if port := extractPortFromAddress(address); port > 0 {
					fmt.Printf("[API] Porta %d extraída do campo '%s': %s\n", port, field, address)
					return port, nil
				}
			}
		}
		
		// 4. Tentar port diretamente em attributes
		if port, ok := attrs["port"].(float64); ok {
			fmt.Printf("[API] Porta %d extraída do campo 'port'\n", int(port))
			return int(port), nil
		}
		
		// 5. Log da estrutura completa para debug (apenas se não encontrou)
		fmt.Printf("[API] [DEBUG] Não foi possível encontrar a porta. Estrutura completa dos attributes:\n")
		jsonData, _ := json.MarshalIndent(attrs, "", "  ")
		fmt.Printf("[API] [DEBUG] %s\n", string(jsonData))
		
		// 6. Tentar encontrar qualquer campo que contenha ":" (pode ser um endereço)
		for key, value := range attrs {
			if strValue, ok := value.(string); ok {
				if strings.Contains(strValue, ":") {
					if port := extractPortFromAddress(strValue); port > 0 {
						fmt.Printf("[API] Porta %d extraída do campo '%s': %s\n", port, key, strValue)
						return port, nil
					}
				}
			}
		}
	}

	// Se chegou aqui, não encontramos a porta em nenhum lugar
	return 0, fmt.Errorf("não foi possível extrair a porta do servidor %s da resposta da API", serverID)
}

// extractPortFromAddress extrai a porta de um endereço no formato "IP:PORTA" ou "host:port"
func extractPortFromAddress(address string) int {
	// Remover protocolo se existir (http://, https://, etc)
	address = strings.TrimPrefix(address, "http://")
	address = strings.TrimPrefix(address, "https://")
	
	// Separar por ":"
	parts := strings.Split(address, ":")
	if len(parts) < 2 {
		return 0
	}
	
	// Pegar a última parte (pode haver IPv6 com múltiplos ":")
	portStr := parts[len(parts)-1]
	
	// Remover qualquer coisa após a porta (como "/path")
	portStr = strings.Split(portStr, "/")[0]
	portStr = strings.Split(portStr, "?")[0]
	
	// Converter para int
	port, err := strconv.Atoi(strings.TrimSpace(portStr))
	if err != nil {
		return 0
	}
	
	if port > 0 && port < 65536 {
		return port
	}
	
	return 0
}

