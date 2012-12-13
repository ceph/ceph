#!/bin/bash -ex

RUN_TIME=300		# approximate duration of run (seconds)

[ $# -eq 1 ] && RUN_TIME="$1"

IMAGE_NAME="image-$$"
IMAGE_SIZE="1024"	# MB

ID_TIMEOUT="10"		# seconds to wait to get rbd id after mapping
ID_DELAY=".1"		# floating-point seconds to delay before rescan

MAP_DELAY=".25"		# floating-point seconds to delay before unmap

function get_time() {
	date '+%s'
}

function times_up() {
	local end_time="$1"

	test $(get_time) -ge "${end_time}"
}

function get_id() {
	[ $# -eq 1 ] || exit 99
	local image_name="$1"
	local id=""
	local end_time=$(expr $(get_time) + ${ID_TIMEOUT})

	cd /sys/bus/rbd/devices

	while [ -z "${id}" ]; do
		if times_up "${end_time}"; then
			break;
		fi
		for i in *; do
			if [ "$(cat $i/name)" = "${image_name}" ]; then
				id=$i
				break
			fi
		done
		sleep "${ID_DELAY}"
	done
	echo $id
	test -n "${id}"		# return code 0 if id was found
}

function map_unmap() {
	[ $# -eq 1 ] || exit 99
	local image_name="$1"

	rbd map "${image_name}"
	RBD_ID=$(get_id "${image_name}")

	sleep "${MAP_DELAY}"

	rbd unmap "/dev/rbd${RBD_ID}"
}

function setup() {
	[ $# -eq 2 ] || exit 99
	local image_name="$1"
	local image_size="$2"

	[ -d /sys/bus/rbd ] || sudo modprobe rbd

	# allow ubuntu user to map/unmap rbd devices
	sudo chown ubuntu /sys/bus/rbd/add
	sudo chown ubuntu /sys/bus/rbd/remove
	rbd create "${image_name}" --size="${image_size}"
}

function cleanup() {
    	# Have to rely on globals for the trap call
	# rbd unmap "/dev/rbd${RBD_ID}"		|| true
	rbd rm "${IMAGE_NAME}"			|| true
	sudo chown root /sys/bus/rbd/remove	|| true
	sudo chown root /sys/bus/rbd/add	|| true
}
trap cleanup EXIT HUP INT

#### Start

setup "${IMAGE_NAME}" "${IMAGE_SIZE}"

COUNT=0
START_TIME=$(get_time)
END_TIME=$(expr $(get_time) + ${RUN_TIME})
while ! times_up "${END_TIME}"; do
	map_unmap "${IMAGE_NAME}"
	COUNT=$(expr $COUNT + 1)
done
ELAPSED=$(expr "$(get_time)" - "${START_TIME}")

echo "${COUNT} iterations completed in ${ELAPSED} seconds"

exit 0
