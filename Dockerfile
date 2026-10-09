# Linux build: the event loop uses epoll here (kqueue on macOS).
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ cmake make && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY CMakeLists.txt .
COPY src src
COPY tests tests
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j && ./build/unit_tests

FROM debian:bookworm-slim
COPY --from=build /src/build/kv-store /usr/local/bin/kv-store
VOLUME /data
EXPOSE 6379
ENTRYPOINT ["kv-store", "--bind", "0.0.0.0", "--port", "6379", "--aof", "/data/appendonly.aof"]
CMD ["--appendfsync", "everysec"]
