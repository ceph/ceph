#!/bin/bash -x

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
rbd feature disable ${IMAGE_NAME} journaling
rbd feature disable ${IMAGE_NAME} fast-diff
rbd feature disable ${IMAGE_NAME} object-map
rbd feature disable ${IMAGE_NAME} exclusive-lock

while is_qemu_running ; do
  echo "*** Enabling all features"
  rbd feature enable ${IMAGE_NAME} exclusive-lock || break
  rbd feature enable ${IMAGE_NAME} journaling || break
  rbd feature enable ${IMAGE_NAME} object-map || break
  rbd feature enable ${IMAGE_NAME} fast-diff || break
  if is_qemu_running ; then
    sleep 60
  fi

  echo "*** Disabling all features"
  rbd feature disable ${IMAGE_NAME} journaling || break
  rbd feature disable ${IMAGE_NAME} fast-diff || break
  rbd feature disable ${IMAGE_NAME} object-map || break
  rbd feature disable ${IMAGE_NAME} exclusive-lock || break
  if is_qemu_running ; then
    sleep 60
  fi
done

if is_qemu_running ; then
    echo "RBD command failed on alive QEMU"
    exit 1
fi
