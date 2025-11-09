package main

import (
	"fmt"
	"maneger/internal/fakeserver"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	port := 25567

	fmt.Printf("Iniciando fake server de teste na porta %d...\n", port)
	fmt.Printf("Mensagem de status: %s\n", fakeserver.StatusMessage)
	fmt.Printf("Mensagem de login: %s\n", fakeserver.LoginMessage)
	fmt.Println("Pressione Ctrl+C para parar")

	// Criar e iniciar fake server usando a mensagem de login
	fakeServer := fakeserver.NewFakeServer(port, fakeserver.LoginMessage)

	if err := fakeServer.Start(); err != nil {
		fmt.Printf("Erro ao iniciar fake server: %v\n", err)
		return
	}

	fmt.Printf("Fake server iniciado com sucesso na porta %d\n", port)
	fmt.Println("Aguardando conexões...")

	// Aguardar sinal de interrupção
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nParando fake server...")
	fakeServer.Stop()
	fmt.Println("Fake server parado")
}
