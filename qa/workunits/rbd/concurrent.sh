#!/bin/bash -e

# Copyright (C) 2013 Inktank Storage, Inc.
#
# This is free software; see the source for copying conditions.
# There is NO warranty; not even for MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.
#
# This is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as
# published by the Free Software Foundation version 2.

# Alex Elder <elder@inktank.com>
# January 29, 2013

################################################################

# The purpose of this test is to exercise paths through the rbd
# code, making sure no bad pointer references or invalid reference
# count operations occur in the face of concurrent activity.
#
# Each pass of the test creates an rbd image, maps it, and writes
# some data into the image.  It also reads some data from all of the
# other images that exist at the time the pass executes.  Finally,
# the image is unmapped and removed.  The image removal completes in
# the background.
#
# An iteration of the test consists of performing some number of
# passes, initating each pass as a background job, and finally
# sleeping for a variable delay.  The delay is initially a specified
# value, but each iteration shortens that proportionally, such that
# the last iteration will not delay at all.
#
# The result exercises concurrent creates and deletes of rbd images,
# writes to new images, reads from both written and unwritten image
# data (including reads concurrent with writes), and attempts to
# unmap images being read.

# Usage: concurrent [-i <iter>] [-c <count>] [-d <delay>]
#
# Exit status:
#     0:  success
#     1:  usage error
#     2:  other runtime error
#    99:  argument count error (programming error)
#   100:  getopt error (internal error)

################################################################

set -x

# Default flag values; RBD_CONCURRENT_ITER names are intended
# to be used in yaml scripts to pass in alternate values, e.g.:
#    env:
#        RBD_CONCURRENT_ITER: 20
#        RBD_CONCURRENT_COUNT: 5
#        RBD_CONCURRENT_DELAY: 3
ITER_DEFAULT=${RBD_CONCURRENT_ITER:-100}
COUNT_DEFAULT=${RBD_CONCURRENT_COUNT:-5}
DELAY_DEFAULT=${RBD_CONCURRENT_DELAY:-5}		# seconds

CEPH_SECRET_FILE=${CEPH_SECRET_FILE:-}
CEPH_ID=${CEPH_ID:-admin}
SECRET_ARGS=""
if [ "${CEPH_SECRET_FILE}" ]; then
	SECRET_ARGS="--secret $CEPH_SECRET_FILE"
fi

################################################################

function setup() {
	ID_MAX_DIR=$(mktemp -d /tmp/image_max_id.XXXXX)
	ID_COUNT_DIR=$(mktemp -d /tmp/image_ids.XXXXXX)
	NAMES_DIR=$(mktemp -d /tmp/image_names.XXXXXX)
	SOURCE_DATA=$(mktemp /tmp/source_data.XXXXXX)

	# Use urandom to generate SOURCE_DATA
        dd if=/dev/urandom of=${SOURCE_DATA} bs=2048 count=66 \
               >/dev/null 2>&1

	# List of rbd id's *not* created by this script
	export INITIAL_RBD_IDS=$(ls /sys/bus/rbd/devices)

	# Set up some environment for normal teuthology test setup.
	# This really should not be necessary but I found it was.

	export CEPH_ARGS=" --name client.0"
}

function cleanup() {
	[ ! "${ID_MAX_DIR}" ] && return
	local id
	local image

	# Unmap mapped devices
	for id in $(rbd_ids); do
		image=$(cat "/sys/bus/rbd/devices/${id}/name")
		rbd_unmap_image "${id}"
		rbd_destroy_image "${image}"
	done
	# Get any leftover images
	for image in $(rbd ls 2>/dev/null); do
		rbd_destroy_image "${image}"
	done
	wait
	sync
	rm -f "${SOURCE_DATA}"
	[ -d "${NAMES_DIR}" ] && rmdir "${NAMES_DIR}"
	echo "Max concurrent rbd image count was $(get_max "${ID_COUNT_DIR}")"
	rm -rf "${ID_COUNT_DIR}"
	echo "Max rbd image id was $(get_max "${ID_MAX_DIR}")"
	rm -rf "${ID_MAX_DIR}"
}

function get_max() {
	[ $# -eq 1 ] || exit 99
	local dir="$1"

	ls -U "${dir}" | sort -n | tail -1
}

trap cleanup HUP INT QUIT

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
	echo "        -i or --iterations" >&2
	echo "            iteration count (1 or more)" >&2
	echo "        -c or --count" >&2
	echo "            images created per iteration (1 or more)" >&2
	echo "        -d or --delay" >&2
	echo "            maximum delay between iterations" >&2
	echo "" >&2
	echo "    defaults:" >&2
	echo "        iterations: ${ITER_DEFAULT}"
	echo "        count: ${COUNT_DEFAULT}"
	echo "        delay: ${DELAY_DEFAULT} (seconds)"
	echo "" >&2

	[ $# -gt 0 ] && exit 1

	exit 0		# This is used for a --help
}

# parse command line arguments
function parseargs() {
	ITER="${ITER_DEFAULT}"
	COUNT="${COUNT_DEFAULT}"
	DELAY="${DELAY_DEFAULT}"

	# Short option flags
	SHORT_OPTS=""
	SHORT_OPTS="${SHORT_OPTS},h"
	SHORT_OPTS="${SHORT_OPTS},i:"
	SHORT_OPTS="${SHORT_OPTS},c:"
	SHORT_OPTS="${SHORT_OPTS},d:"

	# Short option flags
	LONG_OPTS=""
	LONG_OPTS="${LONG_OPTS},help"
	LONG_OPTS="${LONG_OPTS},iterations:"
	LONG_OPTS="${LONG_OPTS},count:"
	LONG_OPTS="${LONG_OPTS},delay:"

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
			-i|--iterations)
				ITER="$2"
				[ "${ITER}" -lt 1 ] &&
					usage "bad iterations value"
				shift
				;;
			-c|--count)
				COUNT="$2"
				[ "${COUNT}" -lt 1 ] &&
					usage "bad count value"
				shift
				;;
			-d|--delay)
				DELAY="$2"
				shift
				;;
			*)
				exit 100	# Internal error
				;;
		esac
		shift
	done
	shift
}

function rbd_ids() {
	[ $# -eq 0 ] || exit 99
	local ids
	local i

	[ -d /sys/bus/rbd ] || return
	ids=" $(echo $(ls /sys/bus/rbd/devices)) "
	for i in ${INITIAL_RBD_IDS}; do
		ids=${ids/ ${i} / }
	done
	echo ${ids}
}

function update_maxes() {
	local ids="$@"
	local last_id
	# These aren't 100% safe against concurrent updates but it
	# should be pretty close
	count=$(echo ${ids} | wc -w)
	touch "${ID_COUNT_DIR}/${count}"
	last_id=${ids% }
	last_id=${last_id##* }
	touch "${ID_MAX_DIR}/${last_id}"
}

function rbd_create_image() {
	[ $# -eq 0 ] || exit 99
	local image=$(basename $(mktemp "${NAMES_DIR}/image.XXXXXX"))

	rbd create "${image}" --size=1024
	echo "${image}"
}

function rbd_image_id() {
	[ $# -eq 1 ] || exit 99
	local image="$1"

	grep -l "${image}" /sys/bus/rbd/devices/*/name 2>/dev/null |
		cut -d / -f 6
}

function rbd_map_image() {
	[ $# -eq 1 ] || exit 99
	local image="$1"
	local id

	sudo rbd map "${image}" --user "${CEPH_ID}" ${SECRET_ARGS} \
		> /dev/null 2>&1

	id=$(rbd_image_id "${image}")
	echo "${id}"
}

function rbd_write_image() {
	[ $# -eq 1 ] || exit 99
	local id="$1"

	# Offset and size here are meant to ensure beginning and end
	# cross both (4K or 64K) page and (4MB) rbd object boundaries.
	# It assumes the SOURCE_DATA file has size 66 * 2048 bytes
	dd if="${SOURCE_DATA}" of="/dev/rbd${id}" bs=2048 seek=2015 \
		> /dev/null 2>&1
}

# All starting and ending offsets here are selected so they are not
# aligned on a (4 KB or 64 KB) page boundary
function rbd_read_image() {
	[ $# -eq 1 ] || exit 99
	local id="$1"

	# First read starting and ending at an offset before any
	# written data.  The osd zero-fills data read from an
	# existing rbd object, but before any previously-written
	# data.
	dd if="/dev/rbd${id}" of=/dev/null bs=2048 count=34 skip=3 \
		> /dev/null 2>&1
	# Next read starting at an offset before any written data,
	# but ending at an offset that includes data that's been
	# written.  The osd zero-fills unwritten data at the
	# beginning of a read.
	dd if="/dev/rbd${id}" of=/dev/null bs=2048 count=34 skip=1983 \
		> /dev/null 2>&1
	# Read the data at offset 2015 * 2048 bytes (where it was
	# written) and make sure it matches the original data.
	cmp --quiet "${SOURCE_DATA}" "/dev/rbd${id}" 0 4126720 ||
		echo "MISMATCH!!!"
	# Now read starting within the pre-written data, but ending
	# beyond it.  The rbd client zero-fills the unwritten
	# portion at the end of a read.
	dd if="/dev/rbd${id}" of=/dev/null bs=2048 count=34 skip=2079 \
		> /dev/null 2>&1
	# Now read starting from an unwritten range within a written
	# rbd object.  The rbd client zero-fills this.
	dd if="/dev/rbd${id}" of=/dev/null bs=2048 count=34 skip=2115 \
		> /dev/null 2>&1
	# Finally read from an unwritten region which would reside
	# in a different (non-existent) osd object.  The osd client
	# zero-fills unwritten data when the target object doesn't
	# exist.
	dd if="/dev/rbd${id}" of=/dev/null bs=2048 count=34 skip=4098 \
		> /dev/null 2>&1
}

function rbd_unmap_image() {
	[ $# -eq 1 ] || exit 99
	local id="$1"

	sudo rbd unmap "/dev/rbd${id}"
}

function rbd_destroy_image() {
	[ $# -eq 1 ] || exit 99
	local image="$1"

	# Don't wait for it to complete, to increase concurrency
	rbd rm "${image}" >/dev/null 2>&1 &
	rm -f "${NAMES_DIR}/${image}"
}

function one_pass() {
	[ $# -eq 0 ] || exit 99
	local image
	local id
	local ids
	local i

	image=$(rbd_create_image)
	id=$(rbd_map_image "${image}")
	ids=$(rbd_ids)
	update_maxes "${ids}"
	for i in ${rbd_ids}; do
		if [ "${i}" -eq "${id}" ]; then
			rbd_write_image "${i}"
		else
			rbd_read_image "${i}"
		fi
	done
	rbd_unmap_image "${id}"
	rbd_destroy_image "${image}"
}

################################################################

parseargs "$@"

setup

for iter in $(seq 1 "${ITER}"); do
	for count in $(seq 1 "${COUNT}"); do
		one_pass &
	done
	# Sleep longer at first, overlap iterations more later.
	# Use awk to get sub-second granularity (see sleep(1)).
	sleep $(echo "${DELAY}" "${iter}" "${ITER}" |
		awk '{ printf("%.2f\n", $1 - $1 * $2 / $3);}')

done
wait

cleanup

exit 0
