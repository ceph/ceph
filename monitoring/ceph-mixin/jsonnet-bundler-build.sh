#!/bin/sh -ex

JSONNET_VERSION="v0.4.0"
OUTPUT_DIR=${1:-$(pwd)}

git clone -b ${JSONNET_VERSION} --depth 1 https://github.com/jsonnet-bundler/jsonnet-bundler
make -C jsonnet-bundler  build
mv jsonnet-bundler/_output/jb ${OUTPUT_DIR}
