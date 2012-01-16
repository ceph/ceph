#!/bin/sh

rados -p rbd load-gen \
    --num-objects 10240 \
    --min-object-size 1 \
    --max-object-size 1048576 \
    --max-ops 128 \
    --max-backlog 128 \
    --percent 50 \
    --run-length 600
