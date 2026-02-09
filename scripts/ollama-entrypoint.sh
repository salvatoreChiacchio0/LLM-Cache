#!/bin/sh

# Entrypoint script per Ollama che controlla automaticamente la GPU
# Questo script viene eseguito automaticamente all'avvio del container

echo "=========================================="
echo "  Ollama Docker - Auto GPU Detection"
echo "=========================================="

# Check if nvidia-smi is available (GPU accessible in container)
if command -v nvidia-smi >/dev/null 2>&1; then
    echo ""
    echo "✓ GPU detected and accessible!"
    nvidia-smi --query-gpu=name,memory.total --format=csv,noheader | head -1
    GPU_COUNT=$(nvidia-smi --list-gpus 2>/dev/null | wc -l)
    echo "  Found $GPU_COUNT GPU(s)"
    echo "  Ollama will use GPU acceleration"
    echo ""
else
    echo ""
    echo "⚠ No GPU detected in container"
    echo "  Ollama will run on CPU (slower but functional)"
    echo "  To enable GPU: install nvidia-container-toolkit on host"
    echo ""
fi

echo "Starting Ollama server..."
echo "  Host: ${OLLAMA_HOST:-0.0.0.0}"
echo "  Port: 11434"
echo "=========================================="
echo ""

# Start Ollama (it automatically detects and uses GPU if available)
exec /bin/ollama serve
