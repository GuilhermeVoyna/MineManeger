package docker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// ContainerManager gerencia containers Docker
type ContainerManager struct {
	imageName string
}

// NewContainerManager cria um novo gerenciador de containers
func NewContainerManager(imageName string) *ContainerManager {
	cm := &ContainerManager{
		imageName: imageName,
	}
	// Verificar se a imagem existe, se não, tentar construir
	cm.ensureImageExists()
	return cm
}

// ensureImageExists verifica se a imagem existe
// NÃO constrói mais automaticamente - a imagem deve existir ou ser construída manualmente
func (cm *ContainerManager) ensureImageExists() {
	if !cm.imageExists() {
		fmt.Printf("[DOCKER] [AVISO] Imagem %s não encontrada!\n", cm.imageName)
		fmt.Printf("[DOCKER] [AVISO] Construa manualmente com: docker build -f Dockerfile.fakeserver -t %s .\n", cm.imageName)
		fmt.Printf("[DOCKER] [AVISO] Ou use uma imagem pré-construída. Continuando mesmo assim...\n")
	} else {
		fmt.Printf("[DOCKER] Imagem %s encontrada\n", cm.imageName)
	}
}

// imageExists verifica se a imagem Docker existe
func (cm *ContainerManager) imageExists() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "images", "--format", "{{.Repository}}:{{.Tag}}", cm.imageName)
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	return strings.TrimSpace(string(output)) == cm.imageName
}

// findManagerNetwork procura a rede onde o container manager está rodando
// Retorna o nome da rede ou string vazia se não encontrar
func (cm *ContainerManager) findManagerNetwork(preferredName string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Verificar se o container manager está rodando
	cmd := exec.CommandContext(ctx, "docker", "ps", "--filter", "name=^mine-manager$", "--format", "{{.Names}}")
	output, err := cmd.Output()
	if err != nil || strings.TrimSpace(string(output)) != "mine-manager" {
		fmt.Printf("[DOCKER] [INFO] Container manager não está rodando, não é possível detectar a rede\n")
		return ""
	}

	// Primeiro, verificar se a rede preferida existe e o manager está nela
	cmd = exec.CommandContext(ctx, "docker", "network", "inspect", preferredName, "--format", "{{.Name}}")
	if err := cmd.Run(); err == nil {
		// Rede preferida existe, verificar se o manager está nela
		cmd = exec.CommandContext(ctx, "docker", "inspect", "mine-manager",
			"--format", fmt.Sprintf("{{index .NetworkSettings.Networks \"%s\"}}", preferredName))
		networkInfo, err := cmd.Output()
		if err == nil && strings.TrimSpace(string(networkInfo)) != "" && strings.TrimSpace(string(networkInfo)) != "<no value>" {
			fmt.Printf("[DOCKER] [INFO] Manager está na rede preferida: %s\n", preferredName)
			return preferredName
		}
	}

	// Procurar em qual rede o container manager está rodando
	// Obter todas as redes do container
	cmd = exec.CommandContext(ctx, "docker", "inspect", "mine-manager",
		"--format", "{{range $key, $value := .NetworkSettings.Networks}}{{$key}} {{end}}")
	output, err = cmd.Output()
	if err == nil {
		networks := strings.TrimSpace(string(output))
		if networks != "" {
			// Retornar a primeira rede encontrada que não seja bridge, host ou none
			networkList := strings.Fields(networks)
			for _, net := range networkList {
				net = strings.TrimSpace(net)
				if net != "bridge" && net != "host" && net != "none" && net != "" {
					fmt.Printf("[DOCKER] [INFO] Manager encontrado na rede: %s\n", net)
					return net
				}
			}
			// Se não encontrou rede customizada, retornar a primeira (mesmo que seja bridge)
			if len(networkList) > 0 {
				net := strings.TrimSpace(networkList[0])
				if net != "" {
					fmt.Printf("[DOCKER] [INFO] Manager encontrado na rede: %s\n", net)
					return net
				}
			}
		}
	}

	fmt.Printf("[DOCKER] [INFO] Não foi possível detectar a rede do manager\n")
	return ""
}

// ensureNetworkExists verifica se a rede Docker existe, criando se necessário
func (cm *ContainerManager) ensureNetworkExists(networkName string) error {
	// Verificar se a rede existe
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "network", "inspect", networkName)
	if err := cmd.Run(); err == nil {
		// Rede existe
		return nil
	}

	// Rede não existe - criar
	fmt.Printf("[DOCKER] [INFO] Rede '%s' não existe. Criando rede...\n", networkName)
	cmd = exec.CommandContext(ctx, "docker", "network", "create", "--driver", "bridge", networkName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("falha ao criar rede '%s': %v, output: %s", networkName, err, string(output))
	}

	fmt.Printf("[DOCKER] [INFO] Rede '%s' criada com sucesso\n", networkName)
	return nil
}

// StartFakeServerContainer inicia um container do fake server
func (cm *ContainerManager) StartFakeServerContainer(containerName string, port int, statusMsg, loginMsg string) error {
	// Verificar se o container já existe
	if cm.containerExists(containerName) {
		// Se existe e está rodando, não fazer nada
		if cm.isContainerRunning(containerName) {
			return nil
		}
		// Se existe mas não está rodando, remover primeiro
		cm.removeContainer(containerName)
	}

	// Verificar se já existe um container escutando nesta porta
	existingContainer := cm.findContainerByPort(port)
	if existingContainer != "" && existingContainer != containerName {
		// Verificar se o container existente está rodando
		if cm.isContainerRunning(existingContainer) {
			// Porta já está em uso por outro container
			return fmt.Errorf("porta %d já está em uso pelo container %s", port, existingContainer)
		}
		// Container existe mas não está rodando - remover
		cm.removeContainer(existingContainer)
	}

	// Garantir que a rede Docker existe antes de criar o container
	// A rede deve ser "mine-network" (nome fixo definido no docker-compose.yml)
	networkName := "mine-network"

	// Tentar encontrar a rede correta (pode ter prefixo do projeto do docker-compose)
	actualNetworkName := cm.findManagerNetwork(networkName)
	if actualNetworkName == "" {
		// Rede não encontrada - criar com o nome esperado
		if err := cm.ensureNetworkExists(networkName); err != nil {
			fmt.Printf("[DOCKER] [AVISO] Falha ao garantir rede '%s': %v\n", networkName, err)
			fmt.Printf("[DOCKER] [AVISO] Tentando criar container mesmo assim...\n")
			// Continuar - pode ser que a rede já exista mas o comando falhou
		}
		actualNetworkName = networkName
	} else {
		fmt.Printf("[DOCKER] [INFO] Usando rede existente: %s\n", actualNetworkName)
		networkName = actualNetworkName
	}

	// Obter endereço do manager para comunicação entre containers Docker
	// Estratégia: usar nome do container na rede Docker
	// Isso evita problemas com HTTPS/proxy quando usando host.docker.internal

	// Prioridade de configuração:
	// 1. MANAGER_CONTAINER_NAME (configuração explícita)
	// 2. MANAGER_HOST (configuração customizada)
	// 3. mine-manager (nome padrão do container no docker-compose)
	// 4. host.docker.internal (fallback apenas se não estiver em rede Docker)

	managerHost := os.Getenv("MANAGER_CONTAINER_NAME")
	if managerHost == "" {
		managerHost = os.Getenv("MANAGER_HOST")
	}
	if managerHost == "" {
		// Se não especificado, usar nome do container do manager
		// Como os fake servers serão adicionados à mesma rede do manager,
		// eles podem se conectar diretamente usando o nome do container
		managerHost = "mine-manager" // Nome do container no docker-compose
	}

	managerPort := os.Getenv("MANAGER_API_PORT")
	if managerPort == "" {
		managerPort = "8080" // Default
	}

	// SEMPRE usar HTTP (não HTTPS) para comunicação interna entre containers
	// A comunicação é feita diretamente na rede Docker, sem proxy reverso
	managerURL := fmt.Sprintf("http://%s:%s", managerHost, managerPort)

	fmt.Printf("[DOCKER] [INFO] Manager API URL configurada: %s\n", managerURL)
	fmt.Printf("[DOCKER] [INFO] Containers fake server serão adicionados à rede '%s'\n", networkName)

	// Construir comando docker run
	// IMPORTANTE: Adicionar à mesma rede Docker do manager
	// Isso permite comunicação HTTP direta entre containers usando nome do container
	args := []string{
		"run",
		"-d",
		"--name", containerName,
		"--restart", "no",
		"-p", fmt.Sprintf("%d:%d", port, port), // Mapear porta do host para o container
		"--network", networkName, // Adicionar à mesma rede do manager (CRÍTICO para comunicação)
		"-e", fmt.Sprintf("MANAGER_API_URL=%s", managerURL),
		"-e", fmt.Sprintf("CONTAINER_NAME=%s", containerName),
		cm.imageName,
		"-port", fmt.Sprintf("%d", port),
		"-status-message", statusMsg,
		"-login-message", loginMsg,
	}

	fmt.Printf("[DOCKER] [INFO] Criando container fake server na rede '%s'\n", networkName)
	fmt.Printf("[DOCKER] [INFO] Container se conectará ao manager via: %s\n", managerURL)

	cmd := exec.Command("docker", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("erro ao iniciar container: %v, output: %s", err, string(output))
	}

	// Aguardar um pouco para garantir que o container iniciou
	time.Sleep(1 * time.Second)

	// Verificar se o container está realmente rodando
	if !cm.isContainerRunning(containerName) {
		// Tentar obter logs do container para debug
		logsCmd := exec.Command("docker", "logs", containerName)
		logsOutput, _ := logsCmd.CombinedOutput()
		return fmt.Errorf("container iniciado mas não está rodando. Logs: %s", string(logsOutput))
	}

	fmt.Printf("[DOCKER] Container %s iniciado e rodando na porta %d\n", containerName, port)

	return nil
}

// StopFakeServerContainer para e remove um container do fake server
// FORÇA a remoção imediata usando docker kill e rm -f
func (cm *ContainerManager) StopFakeServerContainer(containerName string) error {
	fmt.Printf("[DOCKER] [INFO] FORÇANDO remoção do container: %s\n", containerName)

	if !cm.containerExists(containerName) {
		fmt.Printf("[DOCKER] [INFO] Container %s não existe, nada a fazer\n", containerName)
		return nil
	}

	// FORÇAR parada do container (SIGKILL) - SEM DELAY
	fmt.Printf("[DOCKER] [INFO] Matando container %s...\n", containerName)
	cmd := exec.Command("docker", "kill", containerName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Ignorar erro se o container já estiver parado
		if !strings.Contains(string(output), "No such container") {
			fmt.Printf("[DOCKER] [AVISO] Erro ao matar container (pode já estar parado): %v, output: %s\n", err, string(output))
		}
	} else {
		fmt.Printf("[DOCKER] [INFO] Container %s morto com sucesso\n", containerName)
	}

	// FORÇAR remoção do container - SEM DELAY
	fmt.Printf("[DOCKER] [INFO] Removendo container %s...\n", containerName)
	cmd = exec.Command("docker", "rm", "-f", containerName)
	output, err = cmd.CombinedOutput()
	if err != nil {
		// Se deu erro, tentar novamente
		if !strings.Contains(string(output), "No such container") {
			fmt.Printf("[DOCKER] [AVISO] Erro ao remover container, tentando novamente: %v, output: %s\n", err, string(output))
			time.Sleep(200 * time.Millisecond)
			cmd = exec.Command("docker", "rm", "-f", containerName)
			output, err = cmd.CombinedOutput()
			if err != nil && !strings.Contains(string(output), "No such container") {
				return fmt.Errorf("falha ao remover container após retry: %v, output: %s", err, string(output))
			}
		}
	}

	fmt.Printf("[DOCKER] [INFO] ✓ Container %s removido com sucesso\n", containerName)
	return nil
}

// IsFakeServerContainerRunning verifica se o container está rodando
func (cm *ContainerManager) IsFakeServerContainerRunning(containerName string) bool {
	return cm.isContainerRunning(containerName)
}

// containerExists verifica se o container existe
func (cm *ContainerManager) containerExists(containerName string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "ps", "-a", "--filter", fmt.Sprintf("name=%s", containerName), "--format", "{{.Names}}")
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	return strings.TrimSpace(string(output)) == containerName
}

// isContainerRunning verifica se o container está rodando
func (cm *ContainerManager) isContainerRunning(containerName string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "ps", "--filter", fmt.Sprintf("name=%s", containerName), "--format", "{{.Names}}")
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	return strings.TrimSpace(string(output)) == containerName
}

// removeContainer remove um container
func (cm *ContainerManager) removeContainer(containerName string) {
	exec.Command("docker", "rm", "-f", containerName).Run()
}

// FindContainerByPort encontra um container fake server que está usando a porta especificada
// Retorna o nome do container ou string vazia se não encontrar
// Esta função é pública para permitir que o monitor verifique qual container está usando a porta
func (cm *ContainerManager) FindContainerByPort(port int) string {
	return cm.findContainerByPort(port)
}

// findContainerByPort encontra um container fake server que está usando a porta especificada
// Retorna o nome do container ou string vazia se não encontrar
func (cm *ContainerManager) findContainerByPort(port int) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Primeiro, listar containers rodando (status "Up")
	cmd := exec.CommandContext(ctx, "docker", "ps", "--filter", "name=fakeserver-", "--format", "{{.Names}}|{{.Ports}}")
	output, err := cmd.Output()
	if err != nil {
		return ""
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, "|", 2)
		if len(parts) < 2 {
			continue
		}
		containerName := strings.TrimSpace(parts[0])
		ports := parts[1]

		// Verificar se a porta está na string de portas
		// Formatos possíveis:
		// - 0.0.0.0:25565->25565/tcp (porta mapeada)
		// - 25565/tcp (apenas porta do container)
		// - :::25565->25565/tcp (IPv6)
		portStr := fmt.Sprintf("%d", port)
		if strings.Contains(ports, fmt.Sprintf(":%s->", portStr)) ||
			strings.Contains(ports, fmt.Sprintf(":%s/", portStr)) ||
			strings.Contains(ports, fmt.Sprintf("->%s/", portStr)) {
			return containerName
		}
	}

	return ""
}

// StopAllFakeServerContainers para e remove todos os containers de fake server
func (cm *ContainerManager) StopAllFakeServerContainers() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Listar todos os containers que começam com "fakeserver-"
	cmd := exec.CommandContext(ctx, "docker", "ps", "-a", "--filter", "name=fakeserver-", "--format", "{{.Names}}")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("erro ao listar containers: %v", err)
	}

	containers := strings.Split(strings.TrimSpace(string(output)), "\n")
	stoppedCount := 0

	for _, containerName := range containers {
		containerName = strings.TrimSpace(containerName)
		if containerName == "" {
			continue
		}

		// Parar e remover container
		if err := cm.StopFakeServerContainer(containerName); err != nil {
			fmt.Printf("[DOCKER] [AVISO] Erro ao parar container %s: %v\n", containerName, err)
		} else {
			stoppedCount++
			fmt.Printf("[DOCKER] [INFO] Container %s parado e removido\n", containerName)
		}
	}

	if stoppedCount > 0 {
		fmt.Printf("[DOCKER] [INFO] Total de %d container(s) de fake server parado(s)\n", stoppedCount)
	}

	return nil
}

// MonitorContainerLogs foi removido - não é mais necessário
// O fake server se comunica diretamente com a API do manager quando detecta login
