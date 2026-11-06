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
    libusb-1.0-0-dev \
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
    python3-numpy \
    python3-pip \
    procps \
    ca-certificates \
    dos2unix \
    sox \
    libsox-fmt-all \
    wget \
    curl \
    unzip \
    patch \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 https://github.com/rtlsdrblog/rtl-sdr-blog.git /tmp/rtl-sdr-blog \
    && mkdir -p /tmp/rtl-sdr-blog/build \
    && cd /tmp/rtl-sdr-blog/build \
    && cmake ../ \
        -DINSTALL_UDEV_RULES=ON \
        -DDETACH_KERNEL_DRIVER=ON \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_INSTALL_LIBDIR=lib/x86_64-linux-gnu \
    && make -j$(nproc) \
    && rm -f /usr/lib/x86_64-linux-gnu/librtlsdr.so* \
    && make install \
    && ldconfig \
    && grep -qa "RTL-SDR Blog V4" /usr/lib/x86_64-linux-gnu/librtlsdr.so.0 \
    && rm -rf /tmp/rtl-sdr-blog

RUN pip3 install --no-cache-dir \
    fastapi \
    uvicorn \
    websockets \
    jinja2 \
    aiofiles

RUN git clone https://gitlab.com/larryth/tetra-kit.git /opt/tetra-kit \
    && cd /opt/tetra-kit/decoder \
    && make clean \
    && make \
    && cd /opt/tetra-kit/codec \
    && make clean \
    && make \
    && cp -f /opt/tetra-kit/codec/cdecoder /usr/local/bin/cdecoder \
    && cp -f /opt/tetra-kit/codec/sdecoder /usr/local/bin/sdecoder \
    && chmod +x /usr/local/bin/cdecoder /usr/local/bin/sdecoder \
    && test -x /usr/local/bin/cdecoder \
    && test -x /usr/local/bin/sdecoder \
    && test -x /opt/tetra-kit/decoder/decoder

RUN mkdir -p /app/src/templates /app/logs/audio /app/data \
    && ln -sfn /app/logs/audio /app/data/audio

LABEL org.opencontainers.image.version="2026.11.06.05"
ENV APP_VERSION=2026.11.06.05
ENV APP_RELEASE_DATE=2026-11-06
ENV PYTHONPATH=/app:/app/src

COPY src/db.py /app/src/db.py
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
