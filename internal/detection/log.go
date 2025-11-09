package detection

import (
	"fmt"
	"regexp"
	"strings"
)

// CheckPlayerJoin detecta conexão de jogador - APENAS "joined the game"
func CheckPlayerJoin(logLine string) bool {
	// Ignorar mensagens de chat (formato: <player> mensagem)
	if strings.Contains(logLine, "<") && strings.Contains(logLine, ">") {
		return false
	}

	// Apenas detectar "joined the game" - padrão específico do Minecraft
	joinedPattern := regexp.MustCompile(`(?i)joined\s+the\s+game`)
	return joinedPattern.MatchString(logLine)
}

// CheckPlayerLeave detecta desconexão de jogador - APENAS "left the game"
func CheckPlayerLeave(logLine string) bool {
	// Ignorar mensagens de chat (formato: <player> mensagem)
	if strings.Contains(logLine, "<") && strings.Contains(logLine, ">") {
		return false
	}

	// Apenas detectar "left the game" - padrão específico do Minecraft
	leftPattern := regexp.MustCompile(`(?i)left\s+the\s+game`)
	return leftPattern.MatchString(logLine)
}

// ParseListCommand parseia resposta do comando "list"
func ParseListCommand(logLine string) (int, bool) {
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

// IsServerStarted detecta se o servidor acabou de iniciar
func IsServerStarted(logLine string) bool {
	return strings.Contains(logLine, "Done (") && strings.Contains(logLine, ")! For help, type")
}

