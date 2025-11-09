package fakeserver

// Mensagens customizáveis do fake server
// Edite este arquivo para alterar as mensagens exibidas aos jogadores

var (
	// StatusMessage é exibida quando o jogador visualiza o servidor na tela de multiplayer
	// Esta mensagem aparece no status do servidor (MOTD)
	StatusMessage = "To start the server, please click to connect."

	// LoginMessage é exibida quando o jogador tenta entrar no servidor (login attempt)
	// Esta mensagem aparece quando o jogador clica para conectar ao servidor
	LoginMessage = "The server is being started, please wait and try again in 60 seconds"
)
