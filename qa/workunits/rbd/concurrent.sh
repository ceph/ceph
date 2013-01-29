#!/bin/bash

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

CEPH_SECRET_FILE=${CEPH_SECRET_FILE:-}
CEPH_ID=${CEPH_ID:-admin}
SECRET_ARGS=""
if [ "${CEPH_SECRET_FILE}" ]; then
	SECRET_ARGS="--secret $CEPH_SECRET_FILE"
fi

# Default flag values; RBD_CONCURRENT_ITER names are intended
# to be used in yaml scripts to pass in alternate values, e.g.:
#    env:
#        RBD_CONCURRENT_ITER: 20
#        RBD_CONCURRENT_COUNT: 5
#        RBD_CONCURRENT_DELAY: 3
ITER_DEFAULT=${RBD_CONCURRENT_ITER:-10}
COUNT_DEFAULT=${RBD_CONCURRENT_COUNT:-2}
DELAY_DEFAULT=${RBD_CONCURRENT_DELAY:-5}		# seconds

################################################################

# List of rbd id's *not* created by this script
export INITIAL_RBD_IDS=$(ls /sys/bus/rbd/devices)

function setup() {
	sudo chown ubuntu /sys/bus/rbd/add /sys/bus/rbd/remove

	# Set up some environment for normal teuthology test setup.
	# This really should not be necessary but I found it was.
	TOP="/tmp/cephtest"
	export CEPH_ARGS="--conf ${TOP}/ceph.conf"
	export CEPH_ARGS="${CEPH_ARGS} --keyring ${TOP}/data/client.0.keyring"
	export CEPH_ARGS="${CEPH_ARGS} --name client.0"

	export LD_LIBRARY_PATH="${TOP}/binary/usr/local/lib:${LD_LIBRARY_PATH}"
	export PATH="${TOP}/binary/usr/local/bin:${PATH}"
	export PATH="${TOP}/binary/usr/local/sbin:${PATH}"
}

function cleanup() {
	[ ! "${NAMESDIR}" ] && return
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
	rmdir "${NAMESDIR}"
	sudo chown root /sys/bus/rbd/add /sys/bus/rbd/remove
}
trap cleanup HUP INT QUIT

NAMESDIR=$(mktemp -d /tmp/image_names.XXXXXX)

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

function rbd_create_image() {
	[ $# -eq 0 ] || exit 99
	local image=$(basename $(mktemp "${NAMESDIR}/image.XXXXXX"))

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

	rbd map "${image}" --user "${CEPH_ID}" ${SECRET_ARGS}

	udevadm settle
	id=$(rbd_image_id "${image}")
	echo "${id}"
}

function rbd_write_image() {
	[ $# -eq 1 ] || exit 99
	local id="$1"

	dd if="/bin/busybox" of="/dev/rbd${id}" bs=65536 count=16 seek=16 \
		> /dev/null 2>&1
}

function rbd_read_image() {
	[ $# -eq 1 ] || exit 99
	local id="$1"

	dd if="/dev/rbd${id}" of=/dev/null bs=65536 count=1 > /dev/null 2>&1
}

function rbd_unmap_image() {
	[ $# -eq 1 ] || exit 99
	local id="$1"

	rbd unmap "/dev/rbd${id}" > /dev/null 2>&1
	udevadm settle
}

function rbd_destroy_image() {
	[ $# -eq 1 ] || exit 99
	local image="$1"

	# Don't wait for it to complete, to increase concurrency
	rbd rm "${image}" >/dev/null 2>&1 &
	rm -f "${NAMESDIR}/${image}"
}

function one_pass() {
	[ $# -eq 0 ] || exit 99
	local image
	local id
	local i

	image=$(rbd_create_image)
	id=$(rbd_map_image "${image}")
	for i in $(rbd_ids); do
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
	# Sleep longer at first, overlap iterations more later
	sleep $(expr "${DELAY}" - "${DELAY}" \* "${iter}" / "${ITER}")
done
wait
cleanup

exit 0
