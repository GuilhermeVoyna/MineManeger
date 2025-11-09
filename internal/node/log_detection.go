package node

import (
	"fmt"
	"regexp"
	"strings"
)

// checkPlayerJoin detecta conexão de jogador - APENAS "joined the game"
func checkPlayerJoin(logLine string) bool {
	// Ignorar mensagens de chat (formato: <player> mensagem)
	if strings.Contains(logLine, "<") && strings.Contains(logLine, ">") {
		return false
	}

	// Apenas detectar "joined the game" - padrão específico do Minecraft
	joinedPattern := regexp.MustCompile(`(?i)joined\s+the\s+game`)
	return joinedPattern.MatchString(logLine)
}

// checkPlayerLeave detecta desconexão de jogador - APENAS "left the game"
func checkPlayerLeave(logLine string) bool {
	// Ignorar mensagens de chat (formato: <player> mensagem)
	if strings.Contains(logLine, "<") && strings.Contains(logLine, ">") {
		return false
	}

	// Apenas detectar "left the game" - padrão específico do Minecraft
	leftPattern := regexp.MustCompile(`(?i)left\s+the\s+game`)
	return leftPattern.MatchString(logLine)
}

// parseListCommand parseia resposta do comando "list"
func parseListCommand(logLine string) (int, bool) {
	// Formato: "There are 1 of a max of 20 players online: gui400"
	listPattern := regexp.MustCompile(`(?i)There are (\d+) of a max of \d+ players online`)
	matches := listPattern.FindStringSubmatch(logLine)
	if len(matches) >= 2 {
		var count int
		fmt.Sscanf(matches[1], "%d", &count)
		return count, true
	}
	return 0, false
}

// isServerStarted detecta se o servidor acabou de iniciar
func isServerStarted(logLine string) bool {
	return strings.Contains(logLine, "Done (") && strings.Contains(logLine, ")! For help, type")
}

