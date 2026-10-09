# syntax=docker/dockerfile:1
ARG DEBIAN_IMAGE=debian:trixie-slim

# Opt-in reproducible C++ SDK image. Docker stays outside CMake's target graph.
FROM ${DEBIAN_IMAGE} AS dev
ENV DEBIAN_FRONTEND=noninteractive CMAKE_GENERATOR=Ninja
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake ninja-build clang lld \
    git ca-certificates pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace

FROM dev AS build
ARG CMAKE_PRESET=core
COPY . .
RUN cmake --preset "${CMAKE_PRESET}" && \
    cmake --build --preset "${CMAKE_PRESET}" --parallel 2

FROM build AS test
ARG CMAKE_PRESET=core
RUN ctest --preset "${CMAKE_PRESET}" --output-on-failure --no-tests=error

# A library, not an application: distribute headers, archives, shared objects,
# and CMake package config, without an invented executable ENTRYPOINT.
FROM build AS package
ARG CMAKE_PRESET=core
RUN cmake --install "out/build/${CMAKE_PRESET}" --prefix /opt/dagflow
