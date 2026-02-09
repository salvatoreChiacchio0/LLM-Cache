#!/bin/bash
# Script bash che controlla automaticamente la GPU prima di docker-compose up
# Versione Linux del corrispondente script PowerShell

echo "=== Auto GPU Detection ==="

has_gpu=false

# Check if nvidia-smi is available
if command -v nvidia-smi &> /dev/null; then
    echo "NVIDIA drivers found."
    nvidia-smi --query-gpu=name --format=csv,noheader | head -1
    
    # Check if Docker can access GPU
    echo "Checking Docker GPU access..."
    if docker run --rm --gpus all nvidia/cuda:11.0-base nvidia-smi &> /dev/null; then
        echo "  Docker can access GPU!"
        has_gpu=true
    else
        echo "  Docker cannot access GPU (check nvidia-container-toolkit installation)"
        echo "  Ollama will run on CPU"
        has_gpu=false
    fi
else
    echo "No NVIDIA GPU detected."
    echo "Ollama will run on CPU."
    has_gpu=false
fi

# Create or remove GPU configuration file
if [ "$has_gpu" = true ]; then
    echo ""
    echo "GPU available - enabling GPU support..."
    # Remove override if exists (CPU mode)
    if [ -f docker-compose.override.yml ]; then
        rm docker-compose.override.yml
        echo "  Removed docker-compose.override.yml"
    fi
    # Ensure GPU config exists
    if [ ! -f docker-compose.gpu.yml ]; then
        cat > docker-compose.gpu.yml << 'EOF'
# File incluso automaticamente quando GPU è disponibile
services:
  ollama:
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: all
              capabilities: [gpu]
EOF
        echo "  Created docker-compose.gpu.yml"
    fi
else
    echo ""
    echo "No GPU - using CPU mode..."
    # Remove GPU config if exists
    if [ -f docker-compose.gpu.yml ]; then
        rm docker-compose.gpu.yml
        echo "  Removed docker-compose.gpu.yml"
    fi
    # Create empty override to ensure no GPU config
    cat > docker-compose.override.yml << 'EOF'
# File creato automaticamente quando GPU non è disponibile
# Gestito automaticamente da auto-gpu-check.sh
EOF
    echo "  Created docker-compose.override.yml (CPU mode)"
fi

echo ""
echo "=== Ready to start ==="
echo ""
