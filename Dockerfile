FROM python:3.7-slim-buster

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc openssh-client python-dev libmagic-dev rsync tabix rename \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install -r requirements.txt \
    && apt-get purge -y --auto-remove gcc python-dev
