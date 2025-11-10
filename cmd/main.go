package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"maneger/internal/api"
	"maneger/internal/config"
	"maneger/internal/docker"
	"maneger/internal/monitor"
	"time"
)

func main() {
	// Carregar configurações das variáveis de ambiente
	if err := config.Load(); err != nil {
		log.Fatalf("Erro ao carregar configurações: %v", err)
	}

	// Configuração
	checkInterval := 5 * time.Second // Verificar servidores a cada 5 segundos
	inactivityTimeout := config.InactivityTimeout

	fmt.Printf("Configurações carregadas:\n")
	fmt.Printf("  - Domain: %s\n", config.Domain)
	fmt.Printf("  - Timeout de inatividade: %v\n", inactivityTimeout)
	fmt.Printf("  - Portas serão obtidas automaticamente do Pterodactyl para cada servidor\n")
	fmt.Println()

	// Criar monitor
	m := monitor.NewServerMonitor(checkInterval, inactivityTimeout)

	// Criar container manager para gerenciar containers de fake server
	containerManager := docker.NewContainerManager("mine-manager-fakeserver:latest")

	// Iniciar API HTTP do manager
	managerAPI := api.NewManagerAPI(
		config.ManagerAPIPort,
		func(containerName string) error {
			return containerManager.StopFakeServerContainer(containerName)
		},
		func(containerName string) {
			// Callback quando container é removido - iniciar servidor via WebSocket
			m.OnContainerRemoved(containerName)
		},
	)
	api.SetManagerAPI(managerAPI)

	// Iniciar API HTTP em goroutine
	go func() {
		if err := managerAPI.Start(); err != nil {
			log.Printf("[MANAGER API] [ERRO] Falha ao iniciar API HTTP: %v\n", err)
		}
	}()

	// Configurar handler de sinais para limpeza ao parar
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	// Iniciar monitor em goroutine
	go func() {
		m.Start()
	}()

	// Aguardar sinal de parada
	<-sigChan
	fmt.Println("\n[MONITOR] Recebido sinal de parada. Limpando recursos...")

	// Parar monitor (isso também para todos os containers de fake server)
	m.Stop()

	// Parar todos os containers de fake server (garantia extra)
	fmt.Println("[CLEANUP] Parando todos os containers de fake server restantes...")
	containerManager.StopAllFakeServerContainers()

	fmt.Println("[MONITOR] Encerrado com sucesso")
}
