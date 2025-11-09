package main

import (
	"maneger/internal/node"
)

func main() {
	// Iniciar monitoramento WebSocket para todos os servidores
	node.MonitorAllServers()
}