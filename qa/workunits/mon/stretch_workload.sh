#!/usr/bin/env bash

set -ex

rados bench -p stretched_rbdpool 10 write -b 4096 --no-cleanup