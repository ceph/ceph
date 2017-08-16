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

# Default command line option values
COUNT="1"
EXPUNGE_FILE=""
DO_RANDOMIZE=""	# false
FSTYP="xfs"
SCRATCH_DEV=""	# MUST BE SPECIFIED
TEST_DEV=""	# MUST BE SPECIFIED
TESTS="-g auto"	# The "auto" group is supposed to be "known good"

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
				FSTYP="$2"
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

# run mkfs on the given device using the specified filesystem type
function do_mkfs() {
	arg_count 1 $#

	local dev="${1}"
	local options

	case "${FSTYP}" in
		xfs)	options="-f" ;;
		ext4)	options="-F" ;;
		btrfs)	options="-f" ;;
	esac

	"mkfs.${FSTYP}" ${options} "${dev}" ||
		err "unable to make ${FSTYP} file system on device \"${dev}\""
}

# top-level setup routine
function setup() {
	arg_count 0 $#

	wget -P "${TESTDIR}" http://download.ceph.com/qa/xfstests.tar.gz
	tar zxf "${TESTDIR}/xfstests.tar.gz" -C "$(dirname "${XFSTESTS_DIR}")"
	mkdir "${TEST_DIR}"
	mkdir "${SCRATCH_MNT}"
	do_mkfs "${TEST_DEV}"
}

# top-level (final) cleanup routine
function cleanup() {
	arg_count 0 $#

	# ensure teuthology can clean up the logs
	chmod -R a+rw "${TESTDIR}/archive"

	findmnt "${TEST_DEV}" && umount "${TEST_DEV}"
	[ -d "${SCRATCH_MNT}" ] && rmdir "${SCRATCH_MNT}"
	[ -d "${TEST_DIR}" ] && rmdir "${TEST_DIR}"
	rm -rf "${XFSTESTS_DIR}"
	rm -f "${TESTDIR}/xfstests.tar.gz"
}

# ################################################################

start_date="$(date)"
parseargs "$@"
[ -n "${TESTDIR}" ] || usage "TESTDIR env variable must be set"
[ -d "${TESTDIR}/archive" ] || usage "\$TESTDIR/archive directory must exist"
TESTDIR="$(readlink -e "${TESTDIR}")"
[ -n "${EXPUNGE_FILE}" ] && EXPUNGE_FILE="$(readlink -e "${EXPUNGE_FILE}")"

XFSTESTS_DIR="/var/lib/xfstests"  # hardcoded into dbench binary
TEST_DIR="/mnt/test_dir"
SCRATCH_MNT="/mnt/scratch_mnt"
MKFS_OPTIONS=""
EXT_MOUNT_OPTIONS="-o block_validity"

trap cleanup EXIT ERR HUP INT QUIT
setup

export TEST_DEV
export TEST_DIR
export SCRATCH_DEV
export SCRATCH_MNT
export FSTYP
export MKFS_OPTIONS
export EXT_MOUNT_OPTIONS

pushd "${XFSTESTS_DIR}"
for (( i = 1 ; i <= "${COUNT}" ; i++ )); do
	[ "${COUNT}" -gt 1 ] && echo "=== Iteration "$i" starting at:  $(date)"

	RESULT_BASE="${TESTDIR}/archive/results-${i}"
	mkdir "${RESULT_BASE}"
	export RESULT_BASE

	EXPUNGE=""
	[ -n "${EXPUNGE_FILE}" ] && EXPUNGE="-E ${EXPUNGE_FILE}"

	RANDOMIZE=""
	[ -n "${DO_RANDOMIZE}" ] && RANDOMIZE="-r"

	# -T output timestamps
	PATH="${PWD}/bin:${PATH}" ./check -T ${RANDOMIZE} ${EXPUNGE} ${TESTS}
	findmnt "${TEST_DEV}" && umount "${TEST_DEV}"

	[ "${COUNT}" -gt 1 ] && echo "=== Iteration "$i" complete at:  $(date)"
done
popd

# cleanup is called via the trap call, above

echo "This xfstests run started at:  ${start_date}"
echo "xfstests run completed at:     $(date)"
[ "${COUNT}" -gt 1 ] && echo "xfstests run consisted of ${COUNT} iterations"
echo OK
