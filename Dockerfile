FROM python:3.9-slim-bookworm

# System deps you mentioned earlier (procps, gcc, libmagic-dev, tabix, etc.)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        procps \
        wget \
        libmagic-dev \
        tabix \
        zlib1g-dev \
        libbz2-dev \
        liblzma-dev \
    && rm -rf /var/lib/apt/lists/*

# Workdir first
WORKDIR /app

# Copy requirements first to leverage Docker layer caching
COPY environments/requirements.txt /app/requirements.txt

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt
    # (optional) && pip check

# then copy code
COPY . /app
ENV PATH="/app:${PATH}"
