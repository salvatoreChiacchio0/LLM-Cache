#!/bin/bash
# Script per scaricare automaticamente il modello Ollama richiesto

MODEL="${1:-gemma2:2b}"

echo "========================================"
echo "  Ollama Model Downloader"
echo "========================================"
echo ""

# Trova il container Ollama
OLLAMA_CONTAINER=$(docker ps --format "{{.Names}}" | grep -i ollama | head -1)

if [ -z "$OLLAMA_CONTAINER" ]; then
    echo "ERROR: Ollama container not found!"
    echo "Make sure Ollama is running: docker-compose up -d ollama"
    exit 1
fi

echo "Found Ollama container: $OLLAMA_CONTAINER"
echo ""

# Verifica se il modello è già presente
echo "Checking if model '$MODEL' is already downloaded..."
EXISTING_MODELS=$(docker exec "$OLLAMA_CONTAINER" ollama list 2>&1)

if echo "$EXISTING_MODELS" | grep -q "$MODEL"; then
    echo "✓ Model '$MODEL' is already available!"
    docker exec "$OLLAMA_CONTAINER" ollama list
    exit 0
fi

echo "Model '$MODEL' not found. Downloading..."
echo "This may take several minutes depending on your internet connection..."
echo ""

# Scarica il modello
docker exec "$OLLAMA_CONTAINER" ollama pull "$MODEL"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Model '$MODEL' downloaded successfully!"
    echo ""
    echo "Available models:"
    docker exec "$OLLAMA_CONTAINER" ollama list
else
    echo ""
    echo "ERROR: Failed to download model '$MODEL'"
    echo "Check the error messages above for details."
    exit 1
fi
