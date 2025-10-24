FROM python:3.9-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends procps gcc wget python-dev libmagic-dev tabix \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --upgrade pip
RUN pip install -r environments/requirements.txt \
    && apt-get purge -y --auto-remove gcc python3-dev
