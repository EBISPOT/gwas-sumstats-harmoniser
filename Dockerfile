FROM python:3.9-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends procps gcc wget python3-dev libmagic-dev tabix \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
ENV PATH="/app:${PATH}"

RUN pip install --upgrade pip
RUN pip install -r environments/requirements.txt \
    && apt-get purge -y --auto-remove gcc python3-dev
