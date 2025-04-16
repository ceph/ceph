#!/bin/bash

install_container_deps() {
    source ./src/script/run-make.sh
    # set JENKINS_HOME in order to have the build container look as much
    # like an existing jenkins build environment as possible
    export JENKINS_HOME=/ceph
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

# If DISTRO_KIND is not already set, derive it from the container's os-release.
if [ -z "$DISTRO_KIND" ]; then
    . /etc/os-release
    DISTRO_KIND="${ID}:${VERSION_ID}"
fi

# Execute a container setup process, installing the packges needed to build
# ceph for the given <branch>~<distro_kind> pair. Some distros need extra
# tools in the container image vs. vm hosts or extra tools needed to build
# packages etc.
case "${CEPH_BASE_BRANCH}~${DISTRO_KIND}" in
    *~*centos*8)
        dnf install -y java-1.8.0-openjdk-headless /usr/bin/{rpmbuild,wget,curl}
        install_container_deps
        dnf_clean
    ;;
    *~*centos*9|*~*centos*10*|*~fedora*)
        dnf install -y /usr/bin/{rpmbuild,wget,curl}
        install_container_deps
        dnf_clean
    ;;
    *~*ubuntu*)
        apt-get update
        apt-get install -y wget reprepro curl software-properties-common lksctp-tools libsctp-dev protobuf-compiler ragel libc-ares-dev
        install_container_deps
    ;;
    *)
        echo "Unknown action, branch or build: ${CEPH_BASE_BRANCH}~${DISTRO_KIND}" >&2
        exit 2
    ;;
esac
