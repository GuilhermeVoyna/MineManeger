package main

import (
	"maneger/internal/monitor"
	"time"
)

func main() {
	// Configuração
	checkInterval := 5 * time.Second       // Verificar servidores a cada 5 segundos
	inactivityTimeout := 90 * time.Second  // Parar servidor após 1:30min (90 segundos) sem jogadores

	// Criar e iniciar monitor
	m := monitor.NewServerMonitor(checkInterval, inactivityTimeout)
	m.Start()
}
