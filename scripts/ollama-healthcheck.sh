#!/bin/sh
# Healthcheck script per Ollama che verifica se il servizio è attivo

# Verifica se il processo ollama serve è in esecuzione
if pgrep -f "ollama serve" >/dev/null 2>&1; then
    # Verifica anche se la porta è in ascolto
    if command -v ss >/dev/null 2>&1; then
        ss -ln | grep -q ":11434" && exit 0
    elif command -v netstat >/dev/null 2>&1; then
        netstat -ln | grep -q ":11434" && exit 0
    else
        # Se il processo è in esecuzione, consideralo healthy
        exit 0
    fi
fi

exit 1
