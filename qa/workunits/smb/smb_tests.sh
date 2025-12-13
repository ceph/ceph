#!/bin/sh

set -ex

HERE=$(dirname $0)
PY=${PYTHON:-python3}
VENV=${HERE}/"_smb_tests_$$"

cleanup() {
    rm -rf "${VENV}"
}

$PY -m venv "${VENV}"
trap cleanup EXIT

cd "${HERE}"
"${VENV}/bin/${PY}" -m pip install pytest smbprotocol
"${VENV}/bin/${PY}" -m pytest -v "$@"
