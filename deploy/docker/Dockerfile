FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    pkg-config \
    libsqlite3-dev \
    python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/mxdb
COPY . .

RUN cmake -S . -B build \
    && cmake --build build -j"$(nproc)"

ENTRYPOINT ["/opt/mxdb/build/featured", "/opt/mxdb/deploy/config/featured.conf.example"]
