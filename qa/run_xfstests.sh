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
# run_xfstests -t /dev/<testdev> -s /dev/<scratchdev> [-f <fstype>] -- <tests>
#   - test device and scratch device will both get trashed
#   - fstypes can be xfs, ext4, or btrfs (xfs default)
#   - tests can be listed individually: generic/001 xfs/008 xfs/009
#     tests can also be specified by group: -g quick
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
XFSTESTS_VERSION="facff609afd6a2ca557c2b679e088982026aa188"
XFSPROGS_REPO="git://oss.sgi.com/xfs/cmds/xfsprogs"
XFSPROGS_VERSION="v3.2.2"
XFSDUMP_REPO="git://oss.sgi.com/xfs/cmds/xfsdump"
XFSDUMP_VERSION="v3.1.4"

# Default command line option values
COUNT="1"
EXPUNGE_FILE=""
DO_RANDOMIZE=""	# false
FS_TYPE="xfs"
SCRATCH_DEV=""	# MUST BE SPECIFIED
TEST_DEV=""	# MUST BE SPECIFIED
TESTS="-g auto"	# The "auto" group is supposed to be "known good"

# We no longer need to set the stripe unit in XFS_MKFS_OPTIONS because recent
# versions of mkfs.xfs autodetect it.

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

# validation function for expunge file argument
function expunge_file_valid() {
	arg_count 1 $#

	test -s "$1"
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
	echo "Usage: ${PROGNAME} <options> -- <tests>" >&2
	echo "" >&2
	echo "    options:" >&2
	echo "        -h or --help" >&2
	echo "            show this message" >&2
	echo "        -c or --count" >&2
	echo "            iteration count (1 or more)" >&2
	echo "        -f or --fs-type" >&2
	echo "            one of: xfs, ext4, btrfs" >&2
	echo "            (default fs-type: xfs)" >&2
	echo "        -r or --randomize" >&2
	echo "            randomize test order" >&2
	echo "        -s or --scratch-dev     (REQUIRED)" >&2
	echo "            name of device used for scratch filesystem" >&2
	echo "        -t or --test-dev        (REQUIRED)" >&2
	echo "            name of device used for test filesystem" >&2
	echo "        -x or --expunge-file" >&2
	echo "            name of file with list of tests to skip" >&2
	echo "    tests:" >&2
	echo "        list of test numbers, e.g.:" >&2
	echo "            generic/001 xfs/008 shared/032 btrfs/009" >&2
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
	SHORT_OPTS="${SHORT_OPTS},r"
	SHORT_OPTS="${SHORT_OPTS},s:"
	SHORT_OPTS="${SHORT_OPTS},t:"
	SHORT_OPTS="${SHORT_OPTS},x:"

	# Long option flags
	LONG_OPTS=""
	LONG_OPTS="${LONG_OPTS},help"
	LONG_OPTS="${LONG_OPTS},count:"
	LONG_OPTS="${LONG_OPTS},fs-type:"
	LONG_OPTS="${LONG_OPTS},randomize"
	LONG_OPTS="${LONG_OPTS},scratch-dev:"
	LONG_OPTS="${LONG_OPTS},test-dev:"
	LONG_OPTS="${LONG_OPTS},expunge-file:"

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
			-r|--randomize)
				DO_RANDOMIZE="t"
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
			-x|--expunge-file)
				expunge_file_valid "$2" ||
					usage "invalid expunge-file '$2'"
				EXPUNGE_FILE="$2"
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

[ -n "${TESTDIR}" ] || usage "TESTDIR env variable must be set"

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
#export XFS_MKFS_OPTIONS="${XFS_MKFS_OPTIONS:--f -l su=65536}"
export EXT4_MKFS_OPTIONS="${EXT4_MKFS_OPTIONS:--F}"
export BTRFS_MKFS_OPTION	# No defaults

XFSTESTS_DIR="/var/lib/xfstests"	# Where the tests live
XFSPROGS_DIR="/tmp/cephtest/xfsprogs-install"
XFSDUMP_DIR="/tmp/cephtest/xfsdump-install"
export PATH="${XFSPROGS_DIR}/sbin:${XFSDUMP_DIR}/sbin:${PATH}"

# download, build, and install xfstests
function install_xfstests() {
	arg_count 0 $#

	local multiple=""
	local ncpu

	pushd "${TESTDIR}"

	git clone "${XFSTESTS_REPO}"

	cd xfstests
	git checkout "${XFSTESTS_VERSION}"

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
	export MNTDIR="/tmp/cephtest"

	# Create mount points for the test and scratch filesystems
	mkdir -p ${MNTDIR}
	local test_dir="$(mktemp -d ${MNTDIR}/test_dir.XXXXXXXXXX)"
	local scratch_dir="$(mktemp -d ${MNTDIR}/scratch_mnt.XXXXXXXXXX)"

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
	rmdir "${TEST_DIR}"
	rmdir "${SCRATCH_MNT}"
	rmdir "${MNTDIR}"
}

function install_xfsprogs() {
	arg_count 0 $#

	pushd "${TESTDIR}"
	git clone ${XFSPROGS_REPO}
	cd xfsprogs
	git checkout ${XFSPROGS_VERSION}
	libtoolize -c `libtoolize -n -i >/dev/null 2>/dev/null && echo -i` -f
	cp include/install-sh .
	aclocal -I m4
	autoconf
	./configure --prefix=${XFSPROGS_DIR}
	make install
	popd
}

function install_xfsdump() {
	arg_count 0 $#

	pushd "${TESTDIR}"
	git clone ${XFSDUMP_REPO}
	cd xfsdump
	git checkout ${XFSDUMP_VERSION}

	# somebody took #define min and #define max out, which breaks the build on
	# ubuntu. we back out this commit here, though that may cause problems with
	# this script down the line.
	git revert -n 5a2985233c390d59d2a9757b119cb0e001c87a96
	libtoolize -c `libtoolize -n -i >/dev/null 2>/dev/null && echo -i` -f
	cp include/install-sh .
	aclocal -I m4
	autoconf
	./configure --prefix=${XFSDUMP_DIR}
	(make -k install || true) # that's right, the install process is broken too
	popd
}

function remove_xfsprogs() {
	arg_count 0 $#

	rm -rf ${TESTDIR}/xfsprogs
	rm -rf ${XFSPROGS_DIR}
}	

function remove_xfsdump() {
	arg_count 0 $#

	rm -rf ${TESTDIR}/xfsdump
	rm -rf ${XFSDUMP_DIR}
}


# top-level setup routine
function setup() {
	arg_count 0 $#

	setup_host_options
	install_xfsprogs
	install_xfsdump
	install_xfstests
	setup_xfstests
}

# top-level (final) cleanup routine
function cleanup() {
	arg_count 0 $#

	cd /
	remove_xfsprogs
	remove_xfsdump
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

	EXPUNGE=""
	[ -n "${EXPUNGE_FILE}" ] && EXPUNGE="-E ${EXPUNGE_FILE}"

	RANDOMIZE=""
	[ -n "${DO_RANDOMIZE}" ] && RANDOMIZE="-r"

	# -T output timestamps
	./check -T ${RANDOMIZE} ${EXPUNGE} ${TESTS}
	status=$?

	[ "${COUNT}" -gt 1 ] && echo "=== Iteration "$i" complete at:  $(date)"
done
popd

# cleanup is called via the trap call, above

echo "This xfstests run started at:  ${start_date}"
echo "xfstests run completed at:     $(date)"
[ "${COUNT}" -gt 1 ] && echo "xfstests run consisted of ${COUNT} iterations"

exit "${status}"
