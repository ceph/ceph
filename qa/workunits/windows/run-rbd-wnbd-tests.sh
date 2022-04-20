#!/usr/bin/env bash
set -ex

DIR="$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)"

source ${DIR}/libvirt_vm/build_utils.sh
source ${DIR}/libvirt_vm/connection_info.sh

# Run the rbd-wnbd tests
scp_upload ${DIR}/test_rbd_wnbd.py /test_rbd_wnbd.py
ssh_exec python.exe /test_rbd_wnbd.py --test-name RbdTest
ssh_exec python.exe /test_rbd_wnbd.py --test-name RbdFioTest
ssh_exec python.exe /test_rbd_wnbd.py --test-name RbdStampTest
