package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	// UserToken é o token de autenticação da API do Pterodactyl
	UserToken string

	// Domain é o domínio do painel Pterodactyl
	Domain string

	// InactivityTimeout é o tempo de inatividade antes de parar o servidor
	InactivityTimeout time.Duration

	// ManagerAPIPort é a porta da API HTTP do manager
	ManagerAPIPort int
)

// Load carrega as configurações das variáveis de ambiente
// Retorna erro se variáveis obrigatórias não estiverem definidas
func Load() error {
	// UserToken - OBRIGATÓRIA
	UserToken = os.Getenv("USER_TOKEN")
	if UserToken == "" {
		return fmt.Errorf("USER_TOKEN não está definida. Configure a variável de ambiente USER_TOKEN com o token da API do Pterodactyl")
	}

	// Domain - OBRIGATÓRIA
	Domain = os.Getenv("DOMAIN")
	if Domain == "" {
		return fmt.Errorf("DOMAIN não está definida. Configure a variável de ambiente DOMAIN com o domínio do painel Pterodactyl (ex: https://painel.seudominio.com.br)")
	}

	// Validar formato do DOMAIN
	if !strings.HasPrefix(Domain, "http://") && !strings.HasPrefix(Domain, "https://") {
		return fmt.Errorf("DOMAIN deve começar com http:// ou https://. Valor fornecido: %s", Domain)
	}

	// Ports não são mais necessárias - são obtidas automaticamente do Pterodactyl para cada servidor

	// InactivityTimeout (default: 60 minutos) - OPCIONAL
	timeoutStr := os.Getenv("INACTIVITY_TIMEOUT_MINUTES")
	if timeoutStr == "" {
		InactivityTimeout = 60 * time.Minute // Default: 60 minutos
	} else {
		timeoutMinutes, err := strconv.Atoi(timeoutStr)
		if err != nil {
			return fmt.Errorf("INACTIVITY_TIMEOUT_MINUTES inválido: %v (deve ser um número inteiro)", err)
		}
		if timeoutMinutes <= 0 {
			return fmt.Errorf("INACTIVITY_TIMEOUT_MINUTES deve ser maior que 0. Valor fornecido: %d", timeoutMinutes)
		}
		InactivityTimeout = time.Duration(timeoutMinutes) * time.Minute
	}

	// ManagerAPIPort (default: 8080) - OPCIONAL
	apiPortStr := os.Getenv("MANAGER_API_PORT")
	if apiPortStr == "" {
		ManagerAPIPort = 8080 // Default: 8080
	} else {
		port, err := strconv.Atoi(apiPortStr)
		if err != nil {
			return fmt.Errorf("MANAGER_API_PORT inválido: %v (deve ser um número inteiro)", err)
		}
		if port <= 0 || port > 65535 {
			return fmt.Errorf("MANAGER_API_PORT deve estar entre 1 e 65535. Valor fornecido: %d", port)
		}
		ManagerAPIPort = port
	}

	return nil
}

// GetUserToken retorna o token do usuário
func GetUserToken() string {
	return UserToken
}

// GetDomain retorna o domínio
func GetDomain() string {
	return Domain
}
