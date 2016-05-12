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
# April 10, 2013

################################################################

# The purpose of this test is to validate that data read from a
# mapped rbd image is what it's expected to be.
#
# By default it creates an image and fills it with some data.  It
# then reads back the data at a series of offsets known to cover
# various situations (such as reading the beginning, end, or the
# entirety of an object, or doing a read that spans multiple
# objects), and stashes the results in a set of local files.
#
# It also creates and maps a snapshot of the original image after
# it's been filled, and reads back the same ranges of data from the
# snapshot.  It then compares the data read back with what was read
# back from the original image, verifying they match.
#
# Clone functionality is tested as well, in which case a clone is
# made of the snapshot, and the same ranges of data are again read
# and compared with the original.  In addition, a snapshot of that
# clone is created, and a clone of *that* snapshot is put through
# the same set of tests.  (Clone testing can be optionally skipped.)

################################################################

# Default parameter values.  Environment variables, if set, will
# supercede these defaults.  Such variables have names that begin
# with "IMAGE_READ_", for e.g. use IMAGE_READ_PAGE_SIZE=65536
# to use 65536 as the page size.

DEFAULT_VERBOSE=true
DEFAULT_TEST_CLONES=true
DEFAULT_LOCAL_FILES=false
DEFAULT_FORMAT=2
DEFAULT_DOUBLE_ORDER=true
DEFAULT_HALF_ORDER=false
DEFAULT_PAGE_SIZE=4096
DEFAULT_OBJECT_ORDER=22
MIN_OBJECT_ORDER=12	# technically 9, but the rbd CLI enforces 12
MAX_OBJECT_ORDER=32

PROGNAME=$(basename $0)

ORIGINAL=original-$$
SNAP1=snap1-$$
CLONE1=clone1-$$
SNAP2=snap2-$$
CLONE2=clone2-$$

function err() {
	if [ $# -gt 0 ]; then
		echo "${PROGNAME}: $@" >&2
	fi
	exit 2
}

function usage() {
	if [ $# -gt 0 ]; then
		echo "" >&2
		echo "${PROGNAME}: $@" >&2
	fi
	echo "" >&2
	echo "Usage: ${PROGNAME} [<options>]" >&2
	echo "" >&2
	echo "options are:" >&2
	echo "    -o object_order" >&2
	echo "        must be ${MIN_OBJECT_ORDER}..${MAX_OBJECT_ORDER}" >&2
	echo "    -p page_size    (in bytes)" >&2
	echo "        note: there must be at least 4 pages per object" >&2
	echo "    -1" >&2
	echo "        test using format 1 rbd images (default)" >&2
	echo "    -2" >&2
	echo "        test using format 2 rbd images" >&2
	echo "    -c" >&2
	echo "        also test rbd clone images (implies format 2)" >&2
	echo "    -d" >&2
	echo "        clone object order double its parent's (format 2)" >&2
	echo "    -h" >&2
	echo "        clone object order half of its parent's (format 2)" >&2
	echo "    -l" >&2
	echo "        use local files rather than rbd images" >&2
	echo "    -v" >&2
	echo "        disable reporting of what's going on" >&2
	echo "" >&2
	exit 1
}

function verbose() {
	[ "${VERBOSE}" = true ] && echo "$@"
	true	# Don't let the verbose test spoil our return value
}

function quiet() {
	"$@" 2> /dev/null
}

function boolean_toggle() {
	[ $# -eq 1 ] || exit 99
	test "$1" = "true" && echo false || echo true
}

function parseargs() {
	local opts="o:p:12clv"
	local lopts="order:,page_size:,local,clone,verbose"
	local parsed
	local clone_order_msg

	# use values from environment if available
	VERBOSE="${IMAGE_READ_VERBOSE:-${DEFAULT_VERBOSE}}"
	TEST_CLONES="${IMAGE_READ_TEST_CLONES:-${DEFAULT_TEST_CLONES}}"
	LOCAL_FILES="${IMAGE_READ_LOCAL_FILES:-${DEFAULT_LOCAL_FILES}}"
	DOUBLE_ORDER="${IMAGE_READ_DOUBLE_ORDER:-${DEFAULT_DOUBLE_ORDER}}"
	HALF_ORDER="${IMAGE_READ_HALF_ORDER:-${DEFAULT_HALF_ORDER}}"
	FORMAT="${IMAGE_READ_FORMAT:-${DEFAULT_FORMAT}}"
	PAGE_SIZE="${IMAGE_READ_PAGE_SIZE:-${DEFAULT_PAGE_SIZE}}"
	OBJECT_ORDER="${IMAGE_READ_OBJECT_ORDER:-${DEFAULT_OBJECT_ORDER}}"

	parsed=$(getopt -o "${opts}" -l "${lopts}" -n "${PROGNAME}" -- "$@") ||
		usage
	eval set -- "${parsed}"
	while true; do
		case "$1" in
		-v|--verbose)
			VERBOSE=$(boolean_toggle "${VERBOSE}");;
		-c|--clone)
			TEST_CLONES=$(boolean_toggle "${TEST_CLONES}");;
		-d|--double)
			DOUBLE_ORDER=$(boolean_toggle "${DOUBLE_ORDER}");;
		-h|--half)
			HALF_ORDER=$(boolean_toggle "${HALF_ORDER}");;
		-l|--local)
			LOCAL_FILES=$(boolean_toggle "${LOCAL_FILES}");;
		-1|-2)
			FORMAT="${1:1}";;
		-p|--page_size)
			PAGE_SIZE="$2"; shift;;
		-o|--order)
			OBJECT_ORDER="$2"; shift;;
		--)
			shift; break;;
		*)
			err "getopt internal error"
		esac
		shift
	done
	[ $# -gt 0 ] && usage "excess arguments ($*)"

	if [ "${TEST_CLONES}" = true ]; then
		# If we're using different object orders for clones,
		# make sure the limits are updated accordingly.  If
		# both "half" and "double" are specified, just
		# ignore them both.
		if [ "${DOUBLE_ORDER}" = true ]; then
			if [ "${HALF_ORDER}" = true ]; then
				DOUBLE_ORDER=false
				HALF_ORDER=false
			else
				((MAX_OBJECT_ORDER -= 2))
			fi
		elif [ "${HALF_ORDER}" = true ]; then
			((MIN_OBJECT_ORDER += 2))
		fi
	fi

	[ "${OBJECT_ORDER}" -lt "${MIN_OBJECT_ORDER}" ] &&
		usage "object order (${OBJECT_ORDER}) must be" \
			"at least ${MIN_OBJECT_ORDER}"
	[ "${OBJECT_ORDER}" -gt "${MAX_OBJECT_ORDER}" ] &&
		usage "object order (${OBJECT_ORDER}) must be" \
			"at most ${MAX_OBJECT_ORDER}"

	if [ "${TEST_CLONES}" = true ]; then
		if [ "${DOUBLE_ORDER}" = true ]; then
			((CLONE1_ORDER = OBJECT_ORDER + 1))
			((CLONE2_ORDER = OBJECT_ORDER + 2))
			clone_order_msg="double"
		elif [ "${HALF_ORDER}" = true ]; then
			((CLONE1_ORDER = OBJECT_ORDER - 1))
			((CLONE2_ORDER = OBJECT_ORDER - 2))
			clone_order_msg="half of"
		else
			CLONE1_ORDER="${OBJECT_ORDER}"
			CLONE2_ORDER="${OBJECT_ORDER}"
			clone_order_msg="the same as"
		fi
	fi

	[ "${TEST_CLONES}" != true ] || FORMAT=2

	OBJECT_SIZE=$(echo "2 ^ ${OBJECT_ORDER}" | bc)
	OBJECT_PAGES=$(echo "${OBJECT_SIZE} / ${PAGE_SIZE}" | bc)
	IMAGE_SIZE=$((2 * 16 * OBJECT_SIZE / (1024 * 1024)))
	[ "${IMAGE_SIZE}" -lt 1 ] && IMAGE_SIZE=1
	IMAGE_OBJECTS=$((IMAGE_SIZE * (1024 * 1024) / OBJECT_SIZE))

	[ "${OBJECT_PAGES}" -lt 4 ] &&
		usage "object size (${OBJECT_SIZE}) must be" \
			"at least 4 * page size (${PAGE_SIZE})"

	echo "parameters for this run:"
	echo "    format ${FORMAT} images will be tested"
	echo "    object order is ${OBJECT_ORDER}, so" \
		"objects are ${OBJECT_SIZE} bytes"
	echo "    page size is ${PAGE_SIZE} bytes, so" \
		"there are are ${OBJECT_PAGES} pages in an object"
	echo "    derived image size is ${IMAGE_SIZE} MB, so" \
		"there are ${IMAGE_OBJECTS} objects in an image"
	if [ "${TEST_CLONES}" = true ]; then
		echo "    clone functionality will be tested"
		echo "    object size for a clone will be ${clone_order_msg}"
		echo "        the object size of its parent image"
	fi

	true	# Don't let the clones test spoil our return value
}

function image_dev_path() {
	[ $# -eq 1 ] || exit 99
	local image_name="$1"

	if [ "${LOCAL_FILES}" = true ]; then
		echo "${TEMP}/${image_name}"
		return
	fi

	echo "/dev/rbd/rbd/${image_name}"
}

function out_data_dir() {
	[ $# -lt 2 ] || exit 99
	local out_data="${TEMP}/data"
	local image_name

	if [ $# -eq 1 ]; then
		image_name="$1"
		echo "${out_data}/${image_name}"
	else
		echo "${out_data}"
	fi
}

function setup() {
	verbose "===== setting up ====="
	TEMP=$(mktemp -d /tmp/rbd_image_read.XXXXX)
	mkdir -p $(out_data_dir)

	# create and fill the original image with some data
	create_image "${ORIGINAL}"
	map_image "${ORIGINAL}"
	fill_original

	# create a snapshot of the original
	create_image_snap "${ORIGINAL}" "${SNAP1}"
	map_image_snap "${ORIGINAL}" "${SNAP1}"

	if [ "${TEST_CLONES}" = true ]; then
		# create a clone of the original snapshot
		create_snap_clone "${ORIGINAL}" "${SNAP1}" \
			"${CLONE1}" "${CLONE1_ORDER}"
		map_image "${CLONE1}"

		# create a snapshot of that clone
		create_image_snap "${CLONE1}" "${SNAP2}"
		map_image_snap "${CLONE1}" "${SNAP2}"

		# create a clone of that clone's snapshot
		create_snap_clone "${CLONE1}" "${SNAP2}" \
			"${CLONE2}" "${CLONE2_ORDER}"
		map_image "${CLONE2}"
	fi
}

function teardown() {
	verbose "===== cleaning up ====="
	if [ "${TEST_CLONES}" = true ]; then
		unmap_image "${CLONE2}"					|| true
		destroy_snap_clone "${CLONE1}" "${SNAP2}" "${CLONE2}"	|| true

		unmap_image_snap "${CLONE1}" "${SNAP2}"			|| true
		destroy_image_snap "${CLONE1}" "${SNAP2}"		|| true

		unmap_image "${CLONE1}"					|| true
		destroy_snap_clone "${ORIGINAL}" "${SNAP1}" "${CLONE1}"	|| true
	fi
	unmap_image_snap "${ORIGINAL}" "${SNAP1}"			|| true
	destroy_image_snap "${ORIGINAL}" "${SNAP1}"			|| true
	unmap_image "${ORIGINAL}"					|| true
	destroy_image "${ORIGINAL}"					|| true

	rm -rf $(out_data_dir)
	rmdir "${TEMP}"
}

function create_image() {
	[ $# -eq 1 ] || exit 99
	local image_name="$1"
	local image_path
	local bytes

	verbose "creating image \"${image_name}\""
	if [ "${LOCAL_FILES}" = true ]; then
		image_path=$(image_dev_path "${image_name}")
		bytes=$(echo "${IMAGE_SIZE} * 1024 * 1024 - 1" | bc)
		quiet dd if=/dev/zero bs=1 count=1 seek="${bytes}" \
			of="${image_path}"
		return
	fi

	rbd create "${image_name}" --image-format "${FORMAT}" \
		--size "${IMAGE_SIZE}" --order "${OBJECT_ORDER}" \
		--image-shared
}

function destroy_image() {
	[ $# -eq 1 ] || exit 99
	local image_name="$1"
	local image_path

	verbose "destroying image \"${image_name}\""
	if [ "${LOCAL_FILES}" = true ]; then
		image_path=$(image_dev_path "${image_name}")
		rm -f "${image_path}"
		return
	fi

	rbd rm "${image_name}"
}

function map_image() {
	[ $# -eq 1 ] || exit 99
	local image_name="$1"		# can be image@snap too

	if [ "${LOCAL_FILES}" = true ]; then
		return
	fi

	sudo rbd map "${image_name}"
}

function unmap_image() {
	[ $# -eq 1 ] || exit 99
	local image_name="$1"		# can be image@snap too
	local image_path

	if [ "${LOCAL_FILES}" = true ]; then
		return
	fi
	image_path=$(image_dev_path "${image_name}")

	if [ -e "${image_path}" ]; then
		sudo rbd unmap "${image_path}"
	fi
}

function map_image_snap() {
	[ $# -eq 2 ] || exit 99
	local image_name="$1"
	local snap_name="$2"
	local image_snap

	if [ "${LOCAL_FILES}" = true ]; then
		return
	fi

	image_snap="${image_name}@${snap_name}"
	map_image "${image_snap}"
}

function unmap_image_snap() {
	[ $# -eq 2 ] || exit 99
	local image_name="$1"
	local snap_name="$2"
	local image_snap

	if [ "${LOCAL_FILES}" = true ]; then
		return
	fi

	image_snap="${image_name}@${snap_name}"
	unmap_image "${image_snap}"
}

function create_image_snap() {
	[ $# -eq 2 ] || exit 99
	local image_name="$1"
	local snap_name="$2"
	local image_snap="${image_name}@${snap_name}"
	local image_path
	local snap_path

	verbose "creating snapshot \"${snap_name}\"" \
		"of image \"${image_name}\""
	if [ "${LOCAL_FILES}" = true ]; then
		image_path=$(image_dev_path "${image_name}")
		snap_path=$(image_dev_path "${image_snap}")

		cp "${image_path}" "${snap_path}"
		return
	fi

	rbd snap create "${image_snap}"
}

function destroy_image_snap() {
	[ $# -eq 2 ] || exit 99
	local image_name="$1"
	local snap_name="$2"
	local image_snap="${image_name}@${snap_name}"
	local snap_path

	verbose "destroying snapshot \"${snap_name}\"" \
		"of image \"${image_name}\""
	if [ "${LOCAL_FILES}" = true ]; then
		snap_path=$(image_dev_path "${image_snap}")
		rm -rf "${snap_path}"
		return
	fi

	rbd snap rm "${image_snap}"
}

function create_snap_clone() {
	[ $# -eq 4 ] || exit 99
	local image_name="$1"
	local snap_name="$2"
	local clone_name="$3"
	local clone_order="$4"
	local image_snap="${image_name}@${snap_name}"
	local snap_path
	local clone_path

	verbose "creating clone image \"${clone_name}\"" \
		"of image snapshot \"${image_name}@${snap_name}\""
	if [ "${LOCAL_FILES}" = true ]; then
		snap_path=$(image_dev_path "${image_name}@${snap_name}")
		clone_path=$(image_dev_path "${clone_name}")

		cp "${snap_path}" "${clone_path}"
		return
	fi

	rbd snap protect "${image_snap}"
	rbd clone --order "${clone_order}" --image-shared \
		"${image_snap}" "${clone_name}"
}

function destroy_snap_clone() {
	[ $# -eq 3 ] || exit 99
	local image_name="$1"
	local snap_name="$2"
	local clone_name="$3"
	local image_snap="${image_name}@${snap_name}"
	local clone_path

	verbose "destroying clone image \"${clone_name}\""
	if [ "${LOCAL_FILES}" = true ]; then
		clone_path=$(image_dev_path "${clone_name}")

		rm -rf "${clone_path}"
		return
	fi

	rbd rm "${clone_name}"
	rbd snap unprotect "${image_snap}"
}

# function that produces "random" data with which to fill the image
function source_data() {
	while quiet dd if=/bin/bash skip=$(($$ % 199)) bs="${PAGE_SIZE}"; do
		:	# Just do the dd
	done
}

function fill_original() {
	local image_path=$(image_dev_path "${ORIGINAL}")

	verbose "filling original image"
	# Fill 16 objects worth of "random" data
	source_data |
	quiet dd bs="${PAGE_SIZE}" count=$((16 * OBJECT_PAGES)) \
		of="${image_path}"
}

function do_read() {
	[ $# -eq 3 -o $# -eq 4 ] || exit 99
	local image_name="$1"
	local offset="$2"
	local length="$3"
	[ "${length}" -gt 0 ] || err "do_read: length must be non-zero"
	local image_path=$(image_dev_path "${image_name}")
	local out_data=$(out_data_dir "${image_name}")
	local range=$(printf "%06u~%04u" "${offset}" "${length}")
	local out_file

	[ $# -eq 4 ] && offset=$((offset + 16 * OBJECT_PAGES))

	verbose "reading \"${image_name}\" pages ${range}"

	out_file="${out_data}/pages_${range}"

	quiet dd bs="${PAGE_SIZE}" skip="${offset}" count="${length}" \
		if="${image_path}" of="${out_file}"
}

function one_pass() {
	[ $# -eq 1 -o $# -eq 2 ] || exit 99
	local image_name="$1"
	local extended
	[ $# -eq 2 ] && extended="true"
	local offset
	local length

	offset=0

	# +-----------+-----------+---
	# |X:X:X...X:X| : : ... : | :
	# +-----------+-----------+---
	length="${OBJECT_PAGES}"
	do_read "${image_name}" "${offset}" "${length}" ${extended}
	offset=$((offset + length))

	# ---+-----------+---
	#  : |X: : ... : | :
	# ---+-----------+---
	length=1
	do_read "${image_name}" "${offset}" "${length}" ${extended}
	offset=$((offset + length))

	# ---+-----------+---
	#  : | :X: ... : | :
	# ---+-----------+---
	length=1
	do_read "${image_name}" "${offset}" "${length}" ${extended}
	offset=$((offset + length))

	# ---+-----------+---
	#  : | : :X...X: | :
	# ---+-----------+---
	length=$((OBJECT_PAGES - 3))
	do_read "${image_name}" "${offset}" "${length}" ${extended}
	offset=$((offset + length))

	# ---+-----------+---
	#  : | : : ... :X| :
	# ---+-----------+---
	length=1
	do_read "${image_name}" "${offset}" "${length}" ${extended}
	offset=$((offset + length))

	# ---+-----------+---
	#  : |X:X:X...X:X| :
	# ---+-----------+---
	length="${OBJECT_PAGES}"
	do_read "${image_name}" "${offset}" "${length}" ${extended}
	offset=$((offset + length))

	offset=$((offset + 1))		# skip 1

	# ---+-----------+---
	#  : | :X:X...X:X| :
	# ---+-----------+---
	length=$((OBJECT_PAGES - 1))
	do_read "${image_name}" "${offset}" "${length}" ${extended}
	offset=$((offset + length))

	# ---+-----------+-----------+---
	#  : |X:X:X...X:X|X: : ... : | :
	# ---+-----------+-----------+---
	length=$((OBJECT_PAGES + 1))
	do_read "${image_name}" "${offset}" "${length}" ${extended}
	offset=$((offset + length))

	# ---+-----------+-----------+---
	#  : | :X:X...X:X|X: : ... : | :
	# ---+-----------+-----------+---
	length="${OBJECT_PAGES}"
	do_read "${image_name}" "${offset}" "${length}" ${extended}
	offset=$((offset + length))

	# ---+-----------+-----------+---
	#  : | :X:X...X:X|X:X: ... : | :
	# ---+-----------+-----------+---
	length=$((OBJECT_PAGES + 1))
	do_read "${image_name}" "${offset}" "${length}" ${extended}
	offset=$((offset + length))

	# ---+-----------+-----------+---
	#  : | : :X...X:X|X:X:X...X:X| :
	# ---+-----------+-----------+---
	length=$((2 * OBJECT_PAGES + 2))
	do_read "${image_name}" "${offset}" "${length}" ${extended}
	offset=$((offset + length))

	offset=$((offset + 1))		# skip 1

	# ---+-----------+-----------+-----
	#  : | :X:X...X:X|X:X:X...X:X|X: :
	# ---+-----------+-----------+-----
	length=$((2 * OBJECT_PAGES))
	do_read "${image_name}" "${offset}" "${length}" ${extended}
	offset=$((offset + length))

	# --+-----------+-----------+--------
	#  : | :X:X...X:X|X:X:X...X:X|X:X: :
	# --+-----------+-----------+--------
	length=2049
	length=$((2 * OBJECT_PAGES + 1))
	do_read "${image_name}" "${offset}" "${length}" ${extended}
	# offset=$((offset + length))
}

function run_using() {
	[ $# -eq 1 ] || exit 99
	local image_name="$1"
	local out_data=$(out_data_dir "${image_name}")

	verbose "===== running using \"${image_name}\" ====="
	mkdir -p "${out_data}"
	one_pass "${image_name}"
	one_pass "${image_name}" extended
}

function compare() {
	[ $# -eq 1 ] || exit 99
	local image_name="$1"
	local out_data=$(out_data_dir "${image_name}")
	local original=$(out_data_dir "${ORIGINAL}")

	verbose "===== comparing \"${image_name}\" ====="
	for i in $(ls "${original}"); do
		verbose compare "\"${image_name}\" \"${i}\""
		cmp "${original}/${i}" "${out_data}/${i}"
	done
	[ "${image_name}" = "${ORIGINAL}" ] || rm -rf "${out_data}"
}

function doit() {
	[ $# -eq 1 ] || exit 99
	local image_name="$1"

	run_using "${image_name}"
	compare "${image_name}"
}

########## Start

parseargs "$@"

trap teardown EXIT HUP INT
setup

run_using "${ORIGINAL}"
doit "${ORIGINAL}@${SNAP1}"
if [ "${TEST_CLONES}" = true ]; then
	doit "${CLONE1}"
	doit "${CLONE1}@${SNAP2}"
	doit "${CLONE2}"
fi
rm -rf $(out_data_dir "${ORIGINAL}")

echo "Success!"

exit 0
