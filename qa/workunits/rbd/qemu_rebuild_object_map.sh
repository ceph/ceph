#!/usr/bin/env bash
set -ex

if [[ -z "${IMAGE_NAME}" ]]; then
  echo image name must be provided
  exit 1
fi

is_qemu_running() {
  rbd status ${IMAGE_NAME} | grep -v "Watchers: none"
}

wait_for_qemu() {
  while ! is_qemu_running ; do
    echo "*** Waiting for QEMU"
    sleep 30
  done
}

wait_for_qemu
rbd feature disable ${IMAGE_NAME} journaling || true
rbd feature disable ${IMAGE_NAME} fast-diff || true
rbd feature disable ${IMAGE_NAME} object-map || true
rbd feature disable ${IMAGE_NAME} exclusive-lock || true

rbd feature enable ${IMAGE_NAME} exclusive-lock
rbd feature enable ${IMAGE_NAME} object-map

while is_qemu_running ; do
  echo "*** Rebuilding object map"
  rbd object-map rebuild ${IMAGE_NAME}

  if is_qemu_running ; then
    sleep 60
  fi
done

