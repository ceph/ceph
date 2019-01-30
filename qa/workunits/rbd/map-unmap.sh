#!/usr/bin/env bash
set -ex

RUN_TIME=300		# approximate duration of run (seconds)

[ $# -eq 1 ] && RUN_TIME="$1"

IMAGE_NAME="image-$$"
IMAGE_SIZE="1024"	# MB

function get_time() {
	date '+%s'
}

function times_up() {
	local end_time="$1"

	test $(get_time) -ge "${end_time}"
}

function map_unmap() {
	[ $# -eq 1 ] || exit 99
	local image_name="$1"

	local dev
	dev="$(sudo rbd map "${image_name}")"
	sudo rbd unmap "${dev}"
}

#### Start

rbd create "${IMAGE_NAME}" --size="${IMAGE_SIZE}"

# disable as suggested from openstack teuthology run
rbd feature disable "${IMAGE_NAME}" object-map fast-diff deep-flatten

COUNT=0
START_TIME=$(get_time)
END_TIME=$(expr $(get_time) + ${RUN_TIME})
while ! times_up "${END_TIME}"; do
	map_unmap "${IMAGE_NAME}"
	COUNT=$(expr $COUNT + 1)
done
ELAPSED=$(expr "$(get_time)" - "${START_TIME}")

rbd rm "${IMAGE_NAME}"

echo "${COUNT} iterations completed in ${ELAPSED} seconds"
