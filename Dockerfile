FROM ubuntu:20.04

LABEL org.opencontainers.image.title="tetra_scanner"
LABEL org.opencontainers.image.description="Multi-channel TETRA data and signaling decoder for MCC 240 / MNC 11"
LABEL org.opencontainers.image.source="https://github.com/Minglarn/tetra_scanner"

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    cmake \
    build-essential \
    pkg-config \
    librtlsdr-dev \
    rtl-sdr \
    gnuradio-dev \
    gr-osmosdr \
    python3-gi \
    gobject-introspection \
    gir1.2-gtk-3.0 \
    zlib1g-dev \
    rapidjson-dev \
    socat \
    jq \
    python3-paho-mqtt \
    python3-pip \
    procps \
    ca-certificates \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    fastapi \
    uvicorn \
    websockets \
    jinja2 \
    aiofiles

RUN git clone https://gitlab.com/larryth/tetra-kit.git /opt/tetra-kit \
    && cd /opt/tetra-kit/decoder \
    && make

RUN mkdir -p /app/src/templates /app/logs

LABEL org.opencontainers.image.version="2026.11.04.6"
ENV APP_VERSION=2026.11.04.6
ENV APP_RELEASE_DATE=2026-11-04

COPY src/tetra_multi_rtl.py /app/src/tetra_multi_rtl.py
COPY src/tetra_parser.py /app/src/tetra_parser.py
COPY src/scanner_orchestrator.py /app/src/scanner_orchestrator.py
COPY src/web_api.py /app/src/web_api.py
COPY src/templates/index.html /app/src/templates/index.html
COPY entrypoint.sh /app/entrypoint.sh

RUN dos2unix /app/entrypoint.sh /app/src/*.py /app/src/templates/*.html \
    && chmod +x /app/entrypoint.sh /app/src/*.py

WORKDIR /app

ENTRYPOINT ["/app/entrypoint.sh"]
