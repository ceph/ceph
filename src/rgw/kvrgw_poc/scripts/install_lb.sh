#!/bin/bash
#
# Ceph - scalable distributed file system
#
# Author: Gabriel BenHanokh <gbenhano@redhat.com>
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#
#!/usr/bin/env bash
set -euo pipefail
if command -v nginx >/dev/null; then
  nginx -v
  exit 0
fi
if command -v dnf >/dev/null; then
  echo "Installing nginx..."
  sudo dnf install -y nginx
elif command -v apt-get >/dev/null; then
  sudo apt-get update && sudo apt-get install -y nginx
else
  echo "Install nginx manually for GW on :9080" >&2
  exit 1
fi
