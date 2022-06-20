#!/bin/sh -ex

JSONNET_VERSION="v0.18.0"
OUTPUT_DIR=${1:-$(pwd)}

git clone -b ${JSONNET_VERSION} --depth 1 https://github.com/google/go-jsonnet.git
cd go-jsonnet
go build ./cmd/jsonnet
go build ./cmd/jsonnetfmt
mv jsonnet jsonnetfmt ${OUTPUT_DIR}
