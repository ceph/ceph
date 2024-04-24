#!/bin/bash

set -e

CEPH_ANSIBLE_VAGRANT_BOX="${CEPH_ANSIBLE_VAGRANT_BOX:-centos/stream9}"

if [[ "${CEPH_ANSIBLE_VAGRANT_BOX}" =~ "centos/stream" ]]; then
  EL_VERSION="${CEPH_ANSIBLE_VAGRANT_BOX: -1}"
  LATEST_IMAGE="$(curl -s https://cloud.centos.org/centos/${EL_VERSION}-stream/x86_64/images/CHECKSUM | sed -nE 's/^SHA256.*\((.*-([0-9]+).*vagrant-libvirt.box)\).*$/\1/p' | sort -u | tail -n1)"
  vagrant box remove "${CEPH_ANSIBLE_VAGRANT_BOX}" --all --force || true
  vagrant box add --force --provider libvirt --name "${CEPH_ANSIBLE_VAGRANT_BOX}" "https://cloud.centos.org/centos/${EL_VERSION}-stream/x86_64/images/${LATEST_IMAGE}" --force
fi

retries=0
until [ $retries -ge 5 ]
do
  echo "Attempting to start VMs. Attempts: $retries"
  timeout 10m vagrant up "$@" && break
  retries=$[$retries+1]
  sleep 5
done

sleep 10
