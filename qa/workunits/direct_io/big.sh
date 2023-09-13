#!/bin/sh -ex

echo "test large (16MB) dio write"
dd if=/dev/zero of=foo.big bs=16M count=1 oflag=direct

echo OK
