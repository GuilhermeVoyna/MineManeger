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
	connectionChan chan struct{}  // Canal para notificar quando há uma tentativa de LOGIN (não status)
	managerClient  *ManagerClient // Cliente para API do manager
}

// NewFakeServer cria um novo servidor falso
func NewFakeServer(port int, message string) *FakeServer {
	return &FakeServer{
		port:           port,
		message:        message,
		stopChan:       make(chan struct{}),
		connectionChan: make(chan struct{}, 1),
		managerClient:  NewManagerClient(),
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
// APENAS notifica a API quando recebe login attempt (nextState=2)
// Status requests (nextState=1) NÃO iniciam o servidor real
func (fs *FakeServer) handleConnection(conn net.Conn) {

	// Sempre garantir que enviamos uma mensagem antes de fechar
	defer func() {
		// Aguardar um pouco para garantir que a mensagem foi enviada
		time.Sleep(200 * time.Millisecond)
		conn.Close()
	}()

	// Configurar timeout
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	log.Printf("[FAKE SERVER] ===== NOVA CONEXÃO RECEBIDA =====")
	log.Printf("[FAKE SERVER] Verificando tipo de conexão (apenas login attempt inicia servidor)")

	// Função auxiliar para notificar API de forma consistente
	// APENAS chamada quando nextState == 2 (login attempt)
	notifyAPI := func(reason string) {
		log.Printf("[FAKE SERVER] [INFO] ===== NOTIFICANDO API PARA INICIAR SERVIDOR REAL =====")
		log.Printf("[FAKE SERVER] [INFO] Razão: %s", reason)
		time.Sleep(500 * time.Millisecond)
		if err := fs.managerClient.RequestRemoveContainer(); err != nil {
			log.Printf("[FAKE SERVER] [ERRO] Falha ao notificar API: %v", err)
			// Tentar novamente após 1 segundo
			time.Sleep(1 * time.Second)
			if err := fs.managerClient.RequestRemoveContainer(); err != nil {
				log.Printf("[FAKE SERVER] [ERRO] Falha novamente ao notificar API: %v", err)
			} else {
				log.Printf("[FAKE SERVER] ✓ API notificada com sucesso na segunda tentativa")
			}
		} else {
			log.Printf("[FAKE SERVER] ✓✓✓ API NOTIFICADA COM SUCESSO ✓✓✓")
			log.Printf("[FAKE SERVER] ✓ Servidor real será iniciado")
		}
	}

	// Ler packet length primeiro (VarInt)
	_, err := readVarInt(conn)
	if err != nil {
		// Erro ao ler - NÃO iniciar servidor (erro de protocolo)
		log.Printf("[FAKE SERVER] [INFO] Erro ao ler packet length: %v", err)
		log.Printf("[FAKE SERVER] Erro de protocolo - servidor real NÃO será iniciado")
		fs.sendStatusResponse(conn)
		return
	}

	// Ler handshake packet ID
	packetID, err := readVarInt(conn)
	if err != nil {
		// Erro ao ler - NÃO iniciar servidor (erro de protocolo)
		log.Printf("[FAKE SERVER] [INFO] Erro ao ler packet ID: %v", err)
		log.Printf("[FAKE SERVER] Erro de protocolo - servidor real NÃO será iniciado")
		fs.sendStatusResponse(conn)
		return
	}

	// Handshake packet ID é 0x00 (mas aceitar qualquer coisa)
	if packetID != 0 {
		// Packet pode ser de outra versão ou formato - aceitar mesmo assim
		log.Printf("[FAKE SERVER] [AVISO] Packet ID diferente de 0 (recebido: %d) - aceitando mesmo assim", packetID)
		// Continuar processamento mesmo com packet ID diferente
	}

	// Ler protocol version (aceitar mesmo com erro)
	_, err = readVarInt(conn)
	if err != nil {
		log.Printf("[FAKE SERVER] [AVISO] Erro ao ler protocol version: %v - continuando mesmo assim", err)
		// Continuar - precisamos ler nextState para determinar se é login attempt
	}

	// Ler server address (string) - aceitar mesmo com erro
	_, err = readString(conn)
	if err != nil {
		log.Printf("[FAKE SERVER] [AVISO] Erro ao ler server address: %v - continuando mesmo assim", err)
		// Continuar - precisamos ler nextState para determinar se é login attempt
	}

	// Ler server port (unsigned short) - aceitar mesmo com erro
	portBytes := make([]byte, 2)
	_, err = conn.Read(portBytes)
	if err != nil {
		log.Printf("[FAKE SERVER] [AVISO] Erro ao ler server port: %v - continuando mesmo assim", err)
		// Continuar - precisamos ler nextState para determinar se é login attempt
	}

	// Ler next state (aceitar qualquer valor, mesmo com erro)
	nextState, err := readVarInt(conn)
	if err != nil {
		// Erro ao ler nextState - NÃO iniciar servidor (erro de protocolo)
		log.Printf("[FAKE SERVER] [INFO] Erro ao ler nextState: %v", err)
		log.Printf("[FAKE SERVER] Erro de protocolo - servidor real NÃO será iniciado")
		fs.sendStatusResponse(conn)
		return
	}

	// Log do nextState para debug
	log.Printf("[FAKE SERVER] Handshake recebido - nextState: %d (1=status, 2=login)", nextState)

	// Processar conforme o tipo de requisição
	// APENAS login attempt (nextState=2) deve iniciar o servidor real
	log.Printf("[FAKE SERVER] ===== CONEXÃO PROCESSADA (nextState=%d) =====", nextState)

	if nextState == 1 {
		// Status request - apenas visualização na tela de multiplayer
		// NÃO iniciar servidor real, apenas responder status
		log.Printf("[FAKE SERVER] Status request (nextState=1) - enviando status response")
		log.Printf("[FAKE SERVER] Status request NÃO inicia servidor real (apenas visualização)")
		fs.sendStatusResponse(conn)
		// Aguardar brevemente e responder ping se houver
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		fs.handlePingRequest(conn)
		// NÃO notificar API - status request não deve iniciar servidor
		log.Printf("[FAKE SERVER] Status request processado. Servidor real NÃO será iniciado.")
		return
	} else if nextState == 2 {
		// Login attempt - jogador tentou se conectar
		// ESTE é o único caso que deve iniciar o servidor real
		log.Printf("[FAKE SERVER] ===== LOGIN ATTEMPT DETECTADO (nextState=2) =====")
		log.Printf("[FAKE SERVER] Login attempt - enviando mensagem de desconexão: %s", fs.message)
		fs.sendDisconnectMessage(conn)
		log.Printf("[FAKE SERVER] Mensagem de desconexão enviada ao cliente")

		// NOTIFICAR API para iniciar servidor real (APENAS para login attempt)
		log.Printf("[FAKE SERVER] ===== NOTIFICANDO API PARA INICIAR SERVIDOR REAL =====")
		notifyAPI("login attempt detectado - jogador tentou conectar")

		log.Printf("[FAKE SERVER] Login attempt processado. Servidor real será iniciado.")
		return
	} else {
		// Estado desconhecido - enviar status response mas NÃO iniciar servidor
		log.Printf("[FAKE SERVER] Estado desconhecido (nextState=%d) - enviando status response", nextState)
		fs.sendStatusResponse(conn)
		// NÃO notificar API - estado desconhecido não deve iniciar servidor
		log.Printf("[FAKE SERVER] Estado desconhecido processado. Servidor real NÃO será iniciado.")
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
