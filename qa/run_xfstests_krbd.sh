#!/bin/bash
#
# This is a wrapper around run_xfstests.sh to provide an expunge file
# suitable for krbd xfstests runs.

set -x

[ -n "${TESTDIR}" ] || export TESTDIR="/tmp/cephtest"
[ -d "${TESTDIR}" ] || mkdir "${TESTDIR}"

SCRIPT="run_xfstests.sh"

if [ -z "${URL_BASE}" ]; then
	URL_BASE="https://git.ceph.com/?p=ceph.git;a=blob_plain;f=qa"
fi

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
	
	generic/038
	generic/042	# zeroes out only the last 4k of test file, but expects
			#  only zeros in the entire file. bug in test?
	generic/046	# _count_extents in common/rc assumes backticks do not
			#  remove newlines. This breaks parsing on some
			#  platforms.
	generic/050	# blockdev --setro right after mkfs returns EBUSY
	generic/078 # RENAME_WHITEOUT was enabled in kernel commit 7dcf5c, but causes
			# a BUG for now
	generic/081	# ubuntu lvm2 doesn't suport --yes argument
	generic/083	# mkfs.xfs -dxize=104857600,agcount=6 fails
			#  when sunit=swidth=8192
	generic/093	# not for Linux
	generic/097	# not for Linux
	generic/099	# not for Linux
	generic/204	# stripe size throws off test's math for when to
			#  expect ENOSPC
	generic/231 # broken for disk and rbd by xfs kernel commit 4162bb
	generic/247 # race between DIO and mmap writes
               # see (https://lists.01.org/pipermail/lkp/2015-March/002459.html)

	shared/272	# not for xfs
	shared/289	# not for xfs

	xfs/007		# sector size math
	xfs/030		# mkfs.xfs -dsize=100m,agcount=6 fails
			#  when sunit=swidth=8192
	xfs/032		# xfs_copy cleans up with pthread_kill (RHBA-2015-0537)
	xfs/042		# stripe size throws off test's math when filling FS
	xfs/051
	xfs/057		# test for IRIX
	xfs/058		# test for IRIX
	xfs/069		# _filter_bmap in common/punch parses incorrectly if
			#  blocks are not stripe-aligned
	xfs/070		# extra output from xfs_repair
	xfs/071		# xfs_repair issue on large offsets (RHBA-2015-0537)
	xfs/073
	xfs/081		# very small mkfs breaks test with sunit=swidth-8192
	xfs/095		# not for Linux
	xfs/096		# checks various mkfs options and chokes on sunit/swidth
	xfs/104		# can't suppress sunit/swidth warnings on mkfs
	xfs/109		# can't suppress sunit/swidth warnings on mkfs
	xfs/167
	xfs/178		# test explicitly checks for stripe width of 0
	xfs/191		# tests NFSv4
	xfs/197		# tests 32-bit machines
	xfs/205		# very small mkfs breaks tests with sunit=swidth=8192
	xfs/242		# _filter_bmap in common/punch parses incorrectly if
			#  blocks are not stripe-aligned
	xfs/261		# bug in mount_xfs involving creation of new quota files
	xfs/279		# sector size math (logical v. physical: BZ836433?)
	xfs/297		# XXX: temporarily expunged due to length
	xfs/300		# SELinux
!

./"${SCRIPT}" -x "$(readlink -f "${EXPUNGE}")" "$@"
STATUS=$?

rm -f "${EXPUNGE}"
rm -f "${SCRIPT}"

exit "${STATUS}"
