FROM ubuntu:24.04

RUN apt-get update && apt-get -y install wget curl git openjdk-11-jdk sudo

# Only mount required folders to avoid large context performance issues
RUN --mount=type=bind,source=/install-deps.sh,target=/app/install-deps.sh \
    --mount=type=bind,source=/src/script,target=/app/src/script \
    --mount=type=bind,source=/debian,target=/app/debian \
    /app/install-deps.sh

# Allow passwordless sudo
RUN apt-get install -y sudo \
    && echo "ubuntu ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

ENV LC_ALL=C
ENV LANG=C

USER ubuntu
