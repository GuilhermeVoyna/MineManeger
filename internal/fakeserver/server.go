package fakeserver

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
)

// Mensagens são definidas em messages.go para fácil customização

// FakeServer representa um servidor falso do Minecraft
type FakeServer struct {
	port           int
	listener       net.Listener
	message        string
	stopChan       chan struct{}
	connectionChan chan struct{} // Canal para notificar quando há uma tentativa de LOGIN (não status)
}

// NewFakeServer cria um novo servidor falso
func NewFakeServer(port int, message string) *FakeServer {
	return &FakeServer{
		port:           port,
		message:        message,
		stopChan:       make(chan struct{}),
		connectionChan: make(chan struct{}, 1),
	}
}

// WaitForConnection aguarda uma conexão (retorna true se houve conexão)
func (fs *FakeServer) WaitForConnection(timeout time.Duration) bool {
	select {
	case <-fs.connectionChan:
		return true
	case <-time.After(timeout):
		return false
	case <-fs.stopChan:
		return false
	}
}

// Start inicia o servidor falso
func (fs *FakeServer) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", fs.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", fs.port, err)
	}

	fs.listener = listener
	fmt.Printf("[FAKE SERVER] Servidor falso iniciado na porta %d\n", fs.port)

	go fs.acceptConnections()

	return nil
}

// Stop para o servidor falso
func (fs *FakeServer) Stop() error {
	close(fs.stopChan)
	if fs.listener != nil {
		return fs.listener.Close()
	}
	return nil
}

// acceptConnections aceita conexões de clientes
func (fs *FakeServer) acceptConnections() {
	for {
		select {
		case <-fs.stopChan:
			return
		default:
			conn, err := fs.listener.Accept()
			if err != nil {
				select {
				case <-fs.stopChan:
					return
				default:
					continue
				}
			}

			go fs.handleConnection(conn)
		}
	}
}

// handleConnection trata uma conexão de cliente
// Sempre garante que uma mensagem de desconexão seja enviada antes de fechar
func (fs *FakeServer) handleConnection(conn net.Conn) {
	// Sempre garantir que enviamos uma mensagem antes de fechar
	defer func() {
		// Aguardar um pouco para garantir que a mensagem foi enviada
		time.Sleep(200 * time.Millisecond)
		conn.Close()
	}()

	// Configurar timeout
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Ler packet length primeiro (VarInt)
	_, err := readVarInt(conn)
	if err != nil {
		// Erro ao ler - tentar enviar status response (pode ser status request)
		fs.sendStatusResponse(conn)
		return
	}

	// Ler handshake packet ID
	packetID, err := readVarInt(conn)
	if err != nil {
		// Erro ao ler - tentar enviar status response (pode ser status request)
		fs.sendStatusResponse(conn)
		return
	}

	// Handshake packet ID é 0x00
	if packetID != 0 {
		// Packet inválido - tentar enviar status response (pode ser status request)
		fs.sendStatusResponse(conn)
		return
	}

	// Ler protocol version
	_, err = readVarInt(conn)
	if err != nil {
		// Erro ao ler - tentar enviar status response (pode ser status request)
		fs.sendStatusResponse(conn)
		return
	}

	// Ler server address (string)
	_, err = readString(conn)
	if err != nil {
		// Erro ao ler - tentar enviar status response (pode ser status request)
		fs.sendStatusResponse(conn)
		return
	}

	// Ler server port (unsigned short)
	portBytes := make([]byte, 2)
	_, err = conn.Read(portBytes)
	if err != nil {
		// Erro ao ler - tentar enviar status response (pode ser status request)
		fs.sendStatusResponse(conn)
		return
	}

	// Ler next state
	nextState, err := readVarInt(conn)
	if err != nil {
		// Erro ao ler - tentar enviar status response (pode ser status request)
		fs.sendStatusResponse(conn)
		return
	}

	// Se next state é 1 (status), responder com status - NÃO notificar (apenas visualização)
	if nextState == 1 {
		// Status request - enviar mensagem de status (visualização na tela de multiplayer)
		log.Printf("[FAKE SERVER] Status request recebido (nextState=1) - enviando mensagem de status: %s", StatusMessage)
		fs.sendStatusResponse(conn)
		// Aguardar brevemente e responder ping se houver
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		fs.handlePingRequest(conn)
		// Aguardar para garantir que mensagem foi enviada
		time.Sleep(200 * time.Millisecond)
		return
	} else if nextState == 2 {
		// Se next state é 2 (login), jogador está tentando ENTRAR no servidor
		log.Printf("[FAKE SERVER] Login attempt recebido (nextState=2) - enviando mensagem de login: %s", fs.message)
		// Notificar que houve uma tentativa de login (não bloqueante)
		select {
		case fs.connectionChan <- struct{}{}:
		default:
		}

		// Enviar mensagem de desconexão (usando mensagem de login - fs.message)
		fs.sendDisconnectMessage(conn)
		// Aguardar para garantir que mensagem foi enviada
		time.Sleep(200 * time.Millisecond)
		return
	} else {
		// Estado desconhecido - enviar status response (mais seguro para visualização)
		log.Printf("[FAKE SERVER] Estado desconhecido (nextState=%d) - enviando mensagem de status: %s", nextState, StatusMessage)
		fs.sendStatusResponse(conn)
		time.Sleep(200 * time.Millisecond)
		return
	}
}

// sendStatusResponse envia resposta de status com a mensagem personalizada
// Sempre garante que a mensagem seja enviada completamente
func (fs *FakeServer) sendStatusResponse(conn net.Conn) {
	// Criar JSON de resposta com mensagem de status (visualização)
	statusResponse := map[string]interface{}{
		"version": map[string]interface{}{
			"name":     "Starting server",
			"protocol": 0,
		},
		"players": map[string]interface{}{
			"max":    0,
			"online": 0,
		},
		"description": map[string]interface{}{
			"text": StatusMessage,
		},
	}

	jsonData, err := json.Marshal(statusResponse)
	if err != nil {
		// Se não conseguir criar JSON, usar mensagem simples com a mensagem de status correta
		jsonData = []byte(fmt.Sprintf(`{"version":{"name":"Starting server","protocol":0},"players":{"max":0,"online":0},"description":{"text":"%s"}}`, StatusMessage))
	}

	// Construir packet de status response (Status Response - packet ID 0x00)
	var packet []byte
	packet = append(packet, 0x00) // Packet ID para Status Response
	packet = writeString(packet, string(jsonData))

	// Enviar packet length (VarInt) + packet
	var lengthBytes []byte
	lengthBytes = appendVarInt(lengthBytes, int32(len(packet)))

	// Enviar tudo de uma vez
	fullPacket := append(lengthBytes, packet...)

	// Escrever e garantir que foi enviado
	_, err = conn.Write(fullPacket)
	if err == nil {
		// Tentar fazer flush se a conexão suportar (TCP já faz isso, mas garantimos)
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetNoDelay(true) // Desabilitar Nagle para envio imediato
		}
	}
}

// sendDisconnectMessage envia mensagem de desconexão (Login Disconnect packet 0x00)
// Esta função sempre garante que a mensagem seja enviada completamente antes de retornar
func (fs *FakeServer) sendDisconnectMessage(conn net.Conn) {
	// Criar JSON de desconexão no formato correto do Minecraft
	// O formato deve ser um objeto JSON com a mensagem
	disconnectMsg := map[string]interface{}{
		"text": fs.message,
	}

	jsonData, err := json.Marshal(disconnectMsg)
	if err != nil {
		// Se não conseguir criar JSON, tentar mensagem simples
		jsonData = []byte(`{"text":"Server is starting, please wait"}`)
	}

	// Construir packet de desconexão (Login Disconnect - packet ID 0x00)
	var packet []byte
	packet = append(packet, 0x00) // Packet ID para Login Disconnect
	packet = writeString(packet, string(jsonData))

	// Enviar packet length (VarInt) + packet
	var lengthBytes []byte
	lengthBytes = appendVarInt(lengthBytes, int32(len(packet)))

	// Enviar tudo de uma vez
	fullPacket := append(lengthBytes, packet...)

	// Escrever e garantir que foi enviado
	_, err = conn.Write(fullPacket)
	if err == nil {
		// Tentar fazer flush se a conexão suportar (TCP já faz isso, mas garantimos)
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetNoDelay(true) // Desabilitar Nagle para envio imediato
		}
	}
}

// handlePingRequest trata ping request do cliente (Status Request - packet 0x01)
func (fs *FakeServer) handlePingRequest(conn net.Conn) {
	// Tentar ler ping request
	packetLength, err := readVarInt(conn)
	if err != nil {
		// Timeout ou erro - não há ping request, tudo bem
		return
	}

	if packetLength <= 0 {
		return
	}

	// Ler packet ID
	packetID, err := readVarInt(conn)
	if err != nil {
		return
	}

	// Se for ping request (0x01), responder com pong (0x01 com o mesmo payload)
	if packetID == 1 {
		// Ler o payload (long - 8 bytes)
		payload := make([]byte, 8)
		_, err = conn.Read(payload)
		if err != nil {
			return
		}

		// Responder com pong (mesmo packet ID e payload)
		var pongPacket []byte
		pongPacket = append(pongPacket, 0x01)       // Packet ID
		pongPacket = append(pongPacket, payload...) // Payload

		// Enviar packet length + packet
		var lengthBytes []byte
		lengthBytes = appendVarInt(lengthBytes, int32(len(pongPacket)))
		fullPacket := append(lengthBytes, pongPacket...)
		conn.Write(fullPacket)
	}
}

// readVarInt lê um VarInt do connection
func readVarInt(conn net.Conn) (int32, error) {
	var value int32
	var position int

	for {
		b := make([]byte, 1)
		_, err := conn.Read(b)
		if err != nil {
			return 0, err
		}

		value |= int32(b[0]&0x7F) << position

		if (b[0] & 0x80) == 0 {
			break
		}

		position += 7
		if position >= 32 {
			return 0, fmt.Errorf("VarInt too long")
		}
	}

	return value, nil
}

// readString lê uma string do connection
func readString(conn net.Conn) (string, error) {
	length, err := readVarInt(conn)
	if err != nil {
		return "", err
	}

	if length < 0 || length > 32767 {
		return "", fmt.Errorf("invalid string length")
	}

	data := make([]byte, length)
	_, err = conn.Read(data)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// writeString escreve uma string no buffer
func writeString(buffer []byte, s string) []byte {
	length := int32(len(s))
	buffer = appendVarInt(buffer, length)
	buffer = append(buffer, []byte(s)...)
	return buffer
}

// appendVarInt adiciona um VarInt ao buffer
func appendVarInt(buffer []byte, value int32) []byte {
	for {
		if (value & ^0x7F) == 0 {
			buffer = append(buffer, byte(value))
			return buffer
		}

		buffer = append(buffer, byte((value&0x7F)|0x80))
		value >>= 7
	}
}
