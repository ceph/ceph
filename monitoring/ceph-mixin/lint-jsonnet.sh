#!/bin/sh -e

JSONNETS_FILES=$(find . -name 'vendor' -prune -o \
                        -name '*.jsonnet' -print -o -name '*.libsonnet' -print)
jsonnetfmt "$@" ${JSONNETS_FILES}
