#!/usr/bin/env bash
#
# TODO switch to run_xfstests.sh (see run_xfstests_krbd.sh)

set -x

[ -n "${TESTDIR}" ] || export TESTDIR="/tmp/cephtest"
[ -d "${TESTDIR}" ] || mkdir "${TESTDIR}"

URL_BASE="https://git.ceph.com/?p=ceph.git;a=blob_plain;f=qa"
SCRIPT="run_xfstests-obsolete.sh"

cd "${TESTDIR}"

curl -O "${URL_BASE}/${SCRIPT}"
# mark executable only if the file isn't empty since ./"${SCRIPT}"
# on an empty file would succeed
if [[ -s "${SCRIPT}" ]]; then
    chmod +x "${SCRIPT}"
fi

TEST_DEV="/dev/vdb"
if [[ ! -b "${TEST_DEV}" ]]; then
    TEST_DEV="/dev/sdb"
fi
SCRATCH_DEV="/dev/vdc"
if [[ ! -b "${SCRATCH_DEV}" ]]; then
    SCRATCH_DEV="/dev/sdc"
fi

# tests excluded fail in the current testing vm regardless of whether
# rbd is used

./"${SCRIPT}" -c 1 -f xfs -t "${TEST_DEV}" -s "${SCRATCH_DEV}" \
    1-7 9-17 19-26 28-49 51-61 63 66-67 69-79 83 85-105 108-110 112-135 \
    137-170 174-191 193-204 206-217 220-227 230-231 233 235-241 243-249 \
    252-259 261-262 264-278 281-286 289
STATUS=$?

rm -f "${SCRIPT}"

exit "${STATUS}"
