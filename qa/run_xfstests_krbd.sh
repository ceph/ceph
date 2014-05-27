#!/bin/bash
#
# This is a wrapper around run_xfstests.sh to provide an expunge file
# suitable for krbd xfstests runs.

set -x

[ -n "${TESTDIR}" ] || export TESTDIR="/tmp/cephtest"
[ -d "${TESTDIR}" ] || mkdir "${TESTDIR}"

URL_BASE="https://ceph.com/git/?p=ceph.git;a=blob_plain;f=qa"
SCRIPT="run_xfstests.sh"

cd "${TESTDIR}"

wget -O "${SCRIPT}" "${URL_BASE}/${SCRIPT}"
chmod +x "${SCRIPT}"

EXPUNGE="$(mktemp expunge.XXXXXXXXXX)"
cat > "${EXPUNGE}" <<-!
	# mv - moved here from the old version of run_xfstests.sh
	#      and rbd_xfstests.yaml
	# wasn't run - like 'mv', but wasn't specifically excluded
	# new test - didn't exist in the xfstests version that was
	#            used by the old version of this script

	generic/062 # mv
	generic/083 # mv
	generic/127 # mv
	generic/204 # mv
	generic/306 # new test

	xfs/007     # new test
	xfs/008     # mv, see 2db20d972125
	xfs/030     # mv
	xfs/042     # mv
	xfs/073     # mv
	xfs/096     # mv
	xfs/104     # mv
	xfs/109     # mv
	xfs/170     # mv
	xfs/178     # mv
	xfs/200     # mv
	xfs/206     # mv
	xfs/229     # mv
	xfs/242     # mv
	xfs/250     # mv
	xfs/279     # wasn't run
	xfs/287     # wasn't run
	xfs/291     # wasn't run
	xfs/292     # wasn't run
	xfs/293     # wasn't run
	xfs/295     # wasn't run
	xfs/296     # wasn't run
	xfs/301     # new test
	xfs/302     # new test
!

./"${SCRIPT}" -x "$(readlink -f "${EXPUNGE}")" "$@"
STATUS=$?

rm -f "${EXPUNGE}"
rm -f "${SCRIPT}"

exit "${STATUS}"
