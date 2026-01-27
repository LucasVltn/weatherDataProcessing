FROM python:3.10-slim

WORKDIR /app

# Copier requirements
COPY pyproject.toml requirements.txt ./

# Mettre pip à jour
RUN pip install --upgrade pip

# Installer toutes les dépendances système nécessaires pour les packages scientifiques
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    git \
    libffi-dev \
    libbz2-dev \
    liblzma-dev \
    libssl-dev \
    zlib1g-dev \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Installer les packages Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier le projet Kedro
COPY . .

CMD ["bash"]