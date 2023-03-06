#!/usr/bin/env bash
set -ex

DIR="$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)"

source ${DIR}/libvirt_vm/build_utils.sh
source ${DIR}/libvirt_vm/connection_info.sh

# Run the Windows tests
scp_upload ${DIR} /windows-workunits
SSH_TIMEOUT=30m ssh_exec powershell.exe -File /windows-workunits/run-tests.ps1
