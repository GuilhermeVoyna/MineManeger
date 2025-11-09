package main

import (
	"fmt"
	"maneger/internal/fakeserver"
	"maneger/internal/monitor"
	"time"
)

func main() {
	// Iniciar fake server de teste na porta 25599
	// Usar a mensagem de login (será usada quando jogador tentar entrar)
	testPort := 25599
	
	fmt.Printf("[TEST] Iniciando fake server de teste na porta %d...\n", testPort)
	testFakeServer := fakeserver.NewFakeServer(testPort, fakeserver.LoginMessage)
	if err := testFakeServer.Start(); err != nil {
		fmt.Printf("[TEST] [ERRO] Falha ao iniciar fake server de teste: %v\n", err)
	} else {
		fmt.Printf("[TEST] Fake server de teste iniciado na porta %d\n", testPort)
	}

	// Configuração
	checkInterval := 5 * time.Second       // Verificar servidores a cada 5 segundos
	inactivityTimeout := 90 * time.Second  // Parar servidor após 1:30min (90 segundos) sem jogadores

	// Criar e iniciar monitor
	m := monitor.NewServerMonitor(checkInterval, inactivityTimeout)
	m.Start()
}
