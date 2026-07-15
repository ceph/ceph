#!/bin/sh

set -ex

HERE=$(dirname "$0")
PY=${PYTHON:-python3}
if [ "${SMB_REUSE_VENV}" ]; then
    VENV="${SMB_REUSE_VENV}"
else
    VENV=${HERE}/"_smb_tests_$$"
fi

cleanup() {
    if [ "${SMB_REUSE_VENV}" ]; then
        return
    fi
    rm -rf "${VENV}"
}

if ! [ -d "${VENV}" ]; then
    $PY -m venv "${VENV}"
fi
trap cleanup EXIT

cd "${HERE}"
"${VENV}/bin/${PY}" -m pip install pytest 'smbprotocol<1.17.0' grpcio grpcio-reflection
"${VENV}/bin/${PY}" -m pytest -v "$@"
