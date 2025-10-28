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

# Copy requirements first to leverage Docker layer caching
COPY requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt
    # (optional) && pip check

# then copy code
COPY . /app
WORKDIR /app
ENV PATH="/app:${PATH}"
