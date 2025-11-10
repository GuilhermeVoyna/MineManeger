package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"maneger/internal/fakeserver"
)

func main() {
	// Parse flags
	port := flag.Int("port", 25565, "Porta para o fake server")
	statusMsg := flag.String("status-message", fakeserver.StatusMessage, "Mensagem de status (visualização)")
	loginMsg := flag.String("login-message", fakeserver.LoginMessage, "Mensagem de login (tentativa de conexão)")
	flag.Parse()

	if *port <= 0 || *port > 65535 {
		log.Fatalf("Porta inválida: %d", *port)
	}

	// Criar fake server com mensagem de login (usada quando jogador tenta entrar)
	fs := fakeserver.NewFakeServer(*port, *loginMsg)

	// Atualizar mensagem de status se fornecida
	if *statusMsg != fakeserver.StatusMessage {
		fakeserver.StatusMessage = *statusMsg
	}

	// Iniciar fake server
	if err := fs.Start(); err != nil {
		log.Fatalf("Erro ao iniciar fake server: %v", err)
	}

	fmt.Printf("Fake server iniciado na porta %d\n", *port)
	fmt.Printf("Status message: %s\n", fakeserver.StatusMessage)
	fmt.Printf("Login message: %s\n", *loginMsg)
	fmt.Println("Pressione Ctrl+C para parar")

	// Aguardar sinal de parada
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nParando fake server...")
	if err := fs.Stop(); err != nil {
		log.Printf("Erro ao parar fake server: %v", err)
	}
}

