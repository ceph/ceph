#!/bin/bash

install_container_deps() {
    source ./src/script/run-make.sh
    prepare
}

dnf_clean() {
    if [ "${CLEAN_DNF}" != no ]; then
        dnf clean all
        rm -rf /var/cache/dnf/*
    fi
}

set -e
export LOCALE=C
cd ${CEPH_CTR_SRC}

case "${CEPH_BRANCH}~${DISTRO}" in
    *~*centos*stream8)
        dnf install -y java-1.8.0-openjdk-headless /usr/bin/rpmbuild wget
        install_container_deps
        dnf_clean
    ;;
    *~*centos*stream9)
        dnf install -y /usr/bin/rpmbuild wget
        install_container_deps
        dnf_clean
    ;;
    *~*ubuntu*22.04)
        apt-get update
        apt-get install -y wget reprepro
        install_container_deps
    ;;
    *)
        echo "Unknown action, branch or build: ${CEPH_BRANCH}~${DISTRO}" >&2
        exit 2
    ;;
esac
