FROM python:3.11-slim

# Imposta la directory di lavoro
WORKDIR /app

# Installa dipendenze di sistema
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements e installa dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia solo il codice sorgente necessario (escludi data/ tramite .dockerignore)
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY kafka-init/ ./kafka-init/
COPY prometheus/ ./prometheus/
COPY grafana/ ./grafana/

# Esponi le porte necessarie
EXPOSE 8000 9090

# Il comando di default può essere sovrascritto da docker-compose
CMD ["python", "-m", "src.services.streamer"]
