#!/bin/bash
# Wrapper script per docker-compose up che controlla automaticamente la GPU
# Versione Linux del corrispondente script PowerShell

echo "========================================"
echo "  LLM-Cache Docker Compose Launcher"
echo "========================================"
echo ""

# Esegui controllo GPU automatico
if [ -f "scripts/auto-gpu-check.sh" ]; then
    echo "Running automatic GPU detection..."
    bash scripts/auto-gpu-check.sh
else
    echo "GPU check script not found, using default CPU mode"
    # Crea override se non esiste
    if [ ! -f docker-compose.override.yml ]; then
        cat > docker-compose.override.yml << 'EOF'
# File creato automaticamente quando GPU non è disponibile
services:
  ollama:
    deploy:
      resources:
        reservations:
          devices: []
EOF
        echo "Created docker-compose.override.yml (CPU mode)"
    fi
fi

echo ""
echo "Starting Docker Compose..."
echo ""

# Determina quali file usare
COMPOSE_FILES=("-f" "docker-compose.yml")
if [ -f "docker-compose.gpu.yml" ]; then
    COMPOSE_FILES+=("-f" "docker-compose.gpu.yml")
    echo "  Including GPU configuration"
fi
if [ -f "docker-compose.override.yml" ]; then
    COMPOSE_FILES+=("-f" "docker-compose.override.yml")
    echo "  Including CPU override"
fi

echo ""

# Esegui docker-compose con tutti gli argomenti passati
if [ $# -eq 0 ]; then
    docker-compose "${COMPOSE_FILES[@]}" up
else
    docker-compose "${COMPOSE_FILES[@]}" up "$@"
fi

# Dopo l'avvio, verifica e scarica il modello se necessario
echo ""
echo "Checking Ollama model..."

sleep 5

OLLAMA_RUNNING=$(docker ps --format "{{.Names}}" | grep -i ollama | head -1)
if [ -n "$OLLAMA_RUNNING" ]; then
    MODEL="${OLLAMA_MODEL:-gemma2:2b}"
    MODEL=$(echo "$MODEL" | xargs)  # trim
    
    echo "Checking if model '$MODEL' is available..."
    MODELS=$(docker exec "$OLLAMA_RUNNING" ollama list 2>&1)
    
    if ! echo "$MODELS" | grep -q "$MODEL"; then
        echo "Model '$MODEL' not found. Downloading..."
        echo "This may take several minutes..."
        docker exec "$OLLAMA_RUNNING" ollama pull "$MODEL"
        
        if [ $? -eq 0 ]; then
            echo "✓ Model '$MODEL' downloaded successfully!"
        else
            echo "⚠ Failed to download model. You can download it manually:"
            echo "  docker exec $OLLAMA_RUNNING ollama pull $MODEL"
        fi
    else
        echo "✓ Model '$MODEL' is already available!"
    fi
else
    echo "Ollama container not running yet. Model will be checked on next startup."
fi
