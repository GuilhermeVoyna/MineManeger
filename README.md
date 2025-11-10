# Mine Manager

An automatic Minecraft server manager integrated with the Pterodactyl panel, designed to optimize server resource usage.

## Description

**Mine Manager** automatically starts and stops Minecraft servers based on player activity.  
It reduces CPU, memory, and power consumption by shutting down inactive servers and starting them again when a player tries to connect.

## Key Features

- Full integration with the **Pterodactyl API**
- Automatically stops servers after a configurable inactivity timeout
- Creates lightweight **fake servers** that stay online to accept connections and trigger the real server startup
- Automatically detects and uses ports configured in Pterodactyl
- Supports execution via **Docker** (recommended) or **direct binary**

## Benefits

- Significant resource savings
- Automatic management of multiple Minecraft instances
- Scalable and lightweight system
- Reduced operational costs for dedicated or VPS hosting

## Requirements

- Access to the Pterodactyl API
- Go 1.21+ (for direct build)
- Docker and Docker Compose (for containerized execution)

## Execution

### Using Docker (recommended)

1. Configure environment variables in `docker-compose.yml`:
   ```yaml
   environment:
     - USER_TOKEN=your_token_here
     - DOMAIN=https://your-panel.com
     - INACTIVITY_TIMEOUT_MINUTES=60
