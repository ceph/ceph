#!/bin/sh -ex

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

while is_qemu_running ; do
  echo "*** Enabling all features"
  rbd feature enable ${IMAGE_NAME} exclusive-lock
  rbd feature enable ${IMAGE_NAME} journaling
  rbd feature enable ${IMAGE_NAME} object-map
  rbd feature enable ${IMAGE_NAME} fast-diff
  if is_qemu_running ; then
    sleep 60
  fi

  echo "*** Disabling all features"
  rbd feature disable ${IMAGE_NAME} journaling
  rbd feature disable ${IMAGE_NAME} fast-diff
  rbd feature disable ${IMAGE_NAME} object-map
  rbd feature disable ${IMAGE_NAME} exclusive-lock
  if is_qemu_running ; then
    sleep 60
  fi
done

