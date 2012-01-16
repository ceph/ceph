#!/bin/sh

    --num-objects 102400 \
rados -p rbd load-gen \
    --min-object-size 1 \
    --max-object-size 1048576 \
    --max-ops 128 \
    --max-backlog 128 \
    --percent 50 \
    --run-length 600
