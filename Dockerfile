FROM python:3.13-slim-bookworm

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

WORKDIR /app

COPY environments/requirements.txt /app/environments/requirements.txt
RUN pip install --no-cache-dir -r /app/environments/requirements.txt

COPY . /app
ENV PATH="/app:${PATH}"
