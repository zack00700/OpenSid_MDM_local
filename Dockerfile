# ═══════════════════════════════════════════════════
# O.S MDM V2 — Dockerfile
# Build : docker build -t osmdm .
# Run   : docker run -p 3000:3000 -p 5001:5001 --env-file .env osmdm
# ═══════════════════════════════════════════════════

FROM python:3.11-slim

# Métadonnées
LABEL maintainer="OpenSID"
LABEL description="O.S MDM V2 — Master Data Management"
LABEL version="2.1.0"

# Variables d'environnement par défaut
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MDM_DEBUG=false

# Dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Dossier de travail
WORKDIR /app

# Installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# Copier le code source
COPY backend/ ./backend/
COPY frontend/ ./frontend/
COPY start.py .

# Créer les dossiers nécessaires
RUN mkdir -p data uploads

# Volumes pour la persistance
VOLUME ["/app/data", "/app/uploads"]

# Exposer les ports
EXPOSE 3000 5001

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:5001/api/health', timeout=5)" || exit 1

# Démarrer l'application
CMD ["python", "start.py"]
