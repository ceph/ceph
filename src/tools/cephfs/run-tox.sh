#!/usr/bin/env bash
TOX_PATH=${CEPH_ROOT}/src/tools/cephfs/tox.ini
CEPHFS=${CEPH_ROOT}/src/tools/cephfs

tox -c ${TOX_PATH} -e flake8
TOX_STATUS="$?"
test "$TOX_STATUS" -ne "0"
rm -rf ${CEPHFS}/.tox/
rm -rf ${CEPHFS}/cephfs_shell.egg-info/
exit $TOX_STATUS
