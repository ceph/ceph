#!/bin/bash

# Copyright (C) 2012 Dreamhost, LLC
#
# This is free software; see the source for copying conditions.
# There is NO warranty; not even for MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.
#
# This is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as
# published by the Free Software Foundation version 2.

# Usage:
# run_xfs_tests -t /dev/<testdev> -s /dev/<scratchdev> -f <fstype> <tests>
#   - test device and scratch device will both get trashed
#   - fstypes can be xfs, ext4, or btrfs (xfs default)
#   - tests can be listed individually or in ranges:  1 3-5 8
#     tests can also be specified by group:           -g quick
#
# Exit status:
#     0:  success
#     1:  usage error
#     2:  other runtime error
#    99:  argument count error (programming error)
#   100:  getopt error (internal error)

# Alex Elder <elder@dreamhost.com>
# April 13, 2012

set -e

PROGNAME=$(basename $0)

# xfstests is downloaded from this git repository and then built.
# XFSTESTS_REPO="git://oss.sgi.com/xfs/cmds/xfstests.git"
XFSTESTS_REPO="git://git.ceph.com/xfstests.git"

# Default command line option values
COUNT="1"
FS_TYPE="xfs"
SCRATCH_DEV=""	# MUST BE SPECIFIED
TEST_DEV=""	# MUST BE SPECIFIED
TESTS="-g auto"	# The "auto" group is supposed to be "known good"

# rbd presents geometry information that causes mkfs.xfs to
# issue a warning.  This option avoids this class of problems.
XFS_MKFS_OPTIONS="-l su=32k"

# Override the default test list with a list of tests known to pass
# until we can work through getting them all passing reliably.
TESTS="1-7 9 11-15 17 19-21 26-29 31-34 41 46-48 50-54 56 61 63-67 69-70 74-76"
TESTS="${TESTS} 78 79 84-89 91-92 100 103 105 108 110 116-121 124 126"
TESTS="${TESTS} 129-135 137-141 164-167 182 184 187-190 192 194"
TESTS="${TESTS} 196 199 201 203 214-216 220-227 234 236-238 241 243-249"
TESTS="${TESTS} 253 257-259 261 262 269 273 275 277 278 280 285 286"
# 275 was the highest available test as of 4/10/12.
# 289 was the highest available test as of 11/15/12.

######
# Some explanation of why tests have been excluded above:
#
# Test 008 was pulled because it contained a race condition leading to
#          spurious failures.
#
# Test 049 was pulled because it caused a kernel fault.
#	http://tracker.newdream.net/issues/2260
# Test 232 was pulled because it caused an XFS error
#	http://tracker.newdream.net/issues/2302
#
# This test passes but takes a LONG time (1+ hours):  127
#
# These were not run for one (anticipated) reason or another:
# 010 016 030 035 040 044 057 058-060 072 077 090 093-095 097-099 104
# 112 113 122 123 125 128 142 147-163 168 175-178 180 185 191 193
# 195 197 198 207-213 217 228 230-233 235 239 240 252 254 255 264-266
# 270-272 276 278-279 281-284 288 289
#
# These tests all failed (produced output different from golden):
# 042 073 083 096 109 169 170 200 202 204-206 218 229 240 242 250
# 263 276 277 279 287
#
# The rest were not part of the "auto" group:
# 018 022 023 024 025 036 037 038 039 043 055 071 080 081 082 101
# 102 106 107 111 114 115 136 171 172 173 251 267 268
######

# print an error message and quit with non-zero status
function err() {
	if [ $# -gt 0 ]; then
		echo "" >&2
		echo "${PROGNAME}: ${FUNCNAME[1]}: $@" >&2
	fi
	exit 2
}

# routine used to validate argument counts to all shell functions
function arg_count() {
	local func
	local want
	local got

	if [ $# -eq 2 ]; then
		func="${FUNCNAME[1]}"	# calling function
		want=$1
		got=$2
	else
		func="${FUNCNAME[0]}"	# i.e., arg_count
		want=2
		got=$#
	fi
	[ "${want}" -eq "${got}" ] && return 0
	echo "${PROGNAME}: ${func}: arg count bad (want ${want} got ${got})" >&2
	exit 99
}

# validation function for repeat count argument
function count_valid() {
	arg_count 1 $#

	test "$1" -gt 0	# 0 is pointless; negative is wrong
}

# validation function for filesystem type argument
function fs_type_valid() {
	arg_count 1 $#

	case "$1" in
		xfs|ext4|btrfs)	return 0 ;;
		*)		return 1 ;;
	esac
}

# validation function for device arguments
function device_valid() {
	arg_count 1 $#

	# Very simple testing--really should try to be more careful...
	test -b "$1"
}

# print a usage message and quit
#
# if a message is supplied, print that first, and then exit
# with non-zero status
function usage() {
	if [ $# -gt 0 ]; then
		echo "" >&2
		echo "$@" >&2
	fi

	echo "" >&2
	echo "Usage: ${PROGNAME} <options> <tests>" >&2
	echo "" >&2
	echo "    options:" >&2
	echo "        -h or --help" >&2
	echo "            show this message" >&2
	echo "        -c or --count" >&2
	echo "            iteration count (1 or more)" >&2
	echo "        -f or --fs-type" >&2
	echo "            one of: xfs, ext4, btrfs" >&2
	echo "            (default fs-type: xfs)" >&2
	echo "        -s or --scratch-dev     (REQUIRED)" >&2
	echo "            name of device used for scratch filesystem" >&2
	echo "        -t or --test-dev        (REQUIRED)" >&2
	echo "            name of device used for test filesystem" >&2
	echo "    tests:" >&2
	echo "        list of test numbers or ranges, e.g.:" >&2
	echo "            1-9 11-15 17 19-21 26-28 31-34 41" >&2
	echo "        or possibly an xfstests test group, e.g.:" >&2
	echo "            -g quick" >&2
	echo "        (default tests: -g auto)" >&2
	echo "" >&2

	[ $# -gt 0 ] && exit 1

	exit 0		# This is used for a --help
}

# parse command line arguments
function parseargs() {
	# Short option flags
	SHORT_OPTS=""
	SHORT_OPTS="${SHORT_OPTS},h"
	SHORT_OPTS="${SHORT_OPTS},c:"
	SHORT_OPTS="${SHORT_OPTS},f:"
	SHORT_OPTS="${SHORT_OPTS},s:"
	SHORT_OPTS="${SHORT_OPTS},t:"

	# Short option flags
	LONG_OPTS=""
	LONG_OPTS="${LONG_OPTS},help"
	LONG_OPTS="${LONG_OPTS},count:"
	LONG_OPTS="${LONG_OPTS},fs-type:"
	LONG_OPTS="${LONG_OPTS},scratch-dev:"
	LONG_OPTS="${LONG_OPTS},test-dev:"

	TEMP=$(getopt --name "${PROGNAME}" \
		--options "${SHORT_OPTS}" \
		--longoptions "${LONG_OPTS}" \
		-- "$@")
	eval set -- "$TEMP"

	while [ "$1" != "--" ]; do
		case "$1" in
			-h|--help)
				usage
				;;
			-c|--count)
				count_valid "$2" ||
					usage "invalid count '$2'"
				COUNT="$2"
				shift
				;;
			-f|--fs-type)
				fs_type_valid "$2" ||
					usage "invalid fs_type '$2'"
				FS_TYPE="$2"
				shift
				;;
			-s|--scratch-dev)
				device_valid "$2" ||
					usage "invalid scratch-dev '$2'"
				SCRATCH_DEV="$2"
				shift
				;;
			-t|--test-dev)
				device_valid "$2" ||
					usage "invalid test-dev '$2'"
				TEST_DEV="$2"
				shift
				;;
			*)
				exit 100	# Internal error
				;;
		esac
		shift
	done
	shift

	[ -n "${TEST_DEV}" ] || usage "test-dev must be supplied"
	[ -n "${SCRATCH_DEV}" ] || usage "scratch-dev must be supplied"

	[ $# -eq 0 ] || TESTS="$@"
}

################################################################

[ -z "$TESTDIR" ] && export TESTDIR="/tmp/cephtest"

# Set up some environment for normal teuthology test setup.
# This really should not be necessary but I found it was.
export CEPH_ARGS="--conf ${TESTDIR}/ceph.conf"
export CEPH_ARGS="${CEPH_ARGS} --keyring ${TESTDIR}/data/client.0.keyring"
export CEPH_ARGS="${CEPH_ARGS} --name client.0"

export LD_LIBRARY_PATH="${TESTDIR}/binary/usr/local/lib:${LD_LIBRARY_PATH}"
export PATH="${TESTDIR}/binary/usr/local/bin:${PATH}"
export PATH="${TESTDIR}/binary/usr/local/sbin:${PATH}"

################################################################

# Filesystem-specific mkfs options--set if not supplied
export XFS_MKFS_OPTIONS="${XFS_MKFS_OPTIONS:--f -l su=65536}"
export EXT4_MKFS_OPTIONS="${EXT4_MKFS_OPTIONS:--F}"
export BTRFS_MKFS_OPTION	# No defaults

XFSTESTS_DIR="/var/lib/xfstests"	# Where the tests live

# download, build, and install xfstests
function install_xfstests() {
	arg_count 0 $#

	local multiple=""
	local ncpu

	pushd "${TESTDIR}"

	git clone "${XFSTESTS_REPO}"

	cd xfstests

	# FIXME: use an older version before the tests were rearranged!
	git reset --hard e5f1a13792f20cfac097fef98007610b422f2cac

	ncpu=$(getconf _NPROCESSORS_ONLN 2>&1)
	[ -n "${ncpu}" -a "${ncpu}" -gt 1 ] && multiple="-j ${ncpu}"

	make realclean
	make ${multiple}
	make -k install

	popd
}

# remove previously-installed xfstests files
function remove_xfstests() {
	arg_count 0 $#

	rm -rf "${TESTDIR}/xfstests"
	rm -rf "${XFSTESTS_DIR}"
}

# create a host options file that uses the specified devices
function setup_host_options() {
	arg_count 0 $#

	# Create mount points for the test and scratch filesystems
	local test_dir="$(mktemp -d ${TESTDIR}/test_dir.XXXXXXXXXX)"
	local scratch_dir="$(mktemp -d ${TESTDIR}/scratch_mnt.XXXXXXXXXX)"

	# Write a host options file that uses these devices.
	# xfstests uses the file defined by HOST_OPTIONS as the
	# place to get configuration variables for its run, and
	# all (or most) of the variables set here are required.
	export HOST_OPTIONS="$(mktemp ${TESTDIR}/host_options.XXXXXXXXXX)"
	cat > "${HOST_OPTIONS}" <<-!
		# Created by ${PROGNAME} on $(date)
		# HOST_OPTIONS="${HOST_OPTIONS}"
		TEST_DEV="${TEST_DEV}"
		SCRATCH_DEV="${SCRATCH_DEV}"
		TEST_DIR="${test_dir}"
		SCRATCH_MNT="${scratch_dir}"
		FSTYP="${FS_TYPE}"
		export TEST_DEV SCRATCH_DEV TEST_DIR SCRATCH_MNT FSTYP
		#
		export XFS_MKFS_OPTIONS="${XFS_MKFS_OPTIONS}"
	!

	# Now ensure we are using the same values
	. "${HOST_OPTIONS}"
}

# remove the host options file, plus the directories it refers to
function cleanup_host_options() {
	arg_count 0 $#

	rm -rf "${TEST_DIR}" "${SCRATCH_MNT}"
	rm -f "${HOST_OPTIONS}"
}

# run mkfs on the given device using the specified filesystem type
function do_mkfs() {
	arg_count 1 $#

	local dev="${1}"
	local options

	case "${FSTYP}" in
		xfs)	options="${XFS_MKFS_OPTIONS}" ;;
		ext4)	options="${EXT4_MKFS_OPTIONS}" ;;
		btrfs)	options="${BTRFS_MKFS_OPTIONS}" ;;
	esac

	"mkfs.${FSTYP}" ${options} "${dev}" ||
		err "unable to make ${FSTYP} file system on device \"${dev}\""
}

# mount the given device on the given mount point
function do_mount() {
	arg_count 2 $#

	local dev="${1}"
	local dir="${2}"

	mount "${dev}" "${dir}" ||
		err "unable to mount file system \"${dev}\" on \"${dir}\""
}

# unmount a previously-mounted device
function do_umount() {
	arg_count 1 $#

	local dev="${1}"

	if mount | grep "${dev}" > /dev/null; then
		if ! umount "${dev}"; then
			err "unable to unmount device \"${dev}\""
		fi
	else
		# Report it but don't error out
		echo "device \"${dev}\" was not mounted" >&2
	fi
}

# do basic xfstests setup--make and mount the test and scratch filesystems
function setup_xfstests() {
	arg_count 0 $#

	# TEST_DEV can persist across test runs, but for now we
	# don't bother.   I believe xfstests prefers its devices to
	# have been already been formatted for the desired
	# filesystem type--it uses blkid to identify things or
	# something.  So we mkfs both here for a fresh start.
	do_mkfs "${TEST_DEV}"
	do_mkfs "${SCRATCH_DEV}"

	# I believe the test device is expected to be mounted; the
	# scratch doesn't need to be (but it doesn't hurt).
	do_mount "${TEST_DEV}" "${TEST_DIR}"
	do_mount "${SCRATCH_DEV}" "${SCRATCH_MNT}"
}

# clean up changes made by setup_xfstests
function cleanup_xfstests() {
	arg_count 0 $#

	# Unmount these in case a test left them mounted (plus
	# the corresponding setup function mounted them...)
	do_umount "${TEST_DEV}"
	do_umount "${SCRATCH_DEV}"
}

# top-level setup routine
function setup() {
	arg_count 0 $#

	setup_host_options
	install_xfstests
	setup_xfstests
}

# top-level (final) cleanup routine
function cleanup() {
	arg_count 0 $#

	cd /
	cleanup_xfstests
	remove_xfstests
	cleanup_host_options
}
trap cleanup EXIT ERR HUP INT QUIT

# ################################################################

start_date="$(date)"

parseargs "$@"

setup

pushd "${XFSTESTS_DIR}"
for (( i = 1 ; i <= "${COUNT}" ; i++ )); do
	[ "${COUNT}" -gt 1 ] && echo "=== Iteration "$i" starting at:  $(date)"

	./check ${TESTS}	# Here we actually run the tests
	status=$?

	[ "${COUNT}" -gt 1 ] && echo "=== Iteration "$i" complete at:  $(date)"
done
popd

# cleanup is called via the trap call, above

echo "This xfstests run started at:  ${start_date}"
echo "xfstests run completed at:     $(date)"
[ "${COUNT}" -gt 1 ] && echo "xfstests run consisted of ${COUNT} iterations"

exit "${status}"
