FROM ubuntu:20.04

LABEL org.opencontainers.image.title="tetra_scanner"
LABEL org.opencontainers.image.description="Multi-channel TETRA data and signaling decoder for MCC 240 / MNC 11"
LABEL org.opencontainers.image.version="2026.11.03.2"
LABEL org.opencontainers.image.source="https://github.com/Minglarn/tetra_scanner"

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV APP_VERSION=2026.11.03.2

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    cmake \
    build-essential \
    pkg-config \
    librtlsdr-dev \
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
    procps \
    ca-certificates \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://gitlab.com/larryth/tetra-kit.git /opt/tetra-kit \
    && cd /opt/tetra-kit/decoder \
    && make

RUN mkdir -p /app/src /app/logs

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV APP_VERSION=2026.11.03.5
ENV APP_RELEASE_DATE=2026-11-03

COPY src/tetra_multi_rtl.py /app/src/tetra_multi_rtl.py
COPY src/tetra_parser.py /app/src/tetra_parser.py
COPY entrypoint.sh /app/entrypoint.sh

RUN dos2unix /app/entrypoint.sh /app/src/*.py \
    && chmod +x /app/entrypoint.sh /app/src/*.py

WORKDIR /app

ENTRYPOINT ["/app/entrypoint.sh"]

