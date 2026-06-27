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
    # EL-ish, 9+
    *~*centos*|*~fedora*|*~rocky*|*~alma*)
        dnf install -y /usr/bin/{rpmbuild,wget,curl}
        install_container_deps
        dnf_clean
    ;;
    # openRuyi's minimal image lacks tar/gzip needed to unpack build tarballs
    *~*openruyi*)
        dnf install -y tar gzip /usr/bin/{rpmbuild,wget,curl}
        install_container_deps
        dnf_clean
    ;;
    *~*ubuntu*|*~*debian*)
        apt-get update
        pkgs=(wget reprepro curl lksctp-tools libsctp-dev protobuf-compiler ragel libc-ares-dev)
        # software-properties-common provides add-apt-repository, which
        # llvm.sh needs to install clang from apt.llvm.org. Debian removed
        # the package in trixie, which ships clang-19 natively instead, so
        # install that directly and run-make.sh will skip llvm.sh.
        if apt-cache show software-properties-common >/dev/null 2>&1; then
            pkgs+=(software-properties-common)
        else
            pkgs+=(clang-19)
        fi
        apt-get install -y "${pkgs[@]}"
        install_container_deps
    ;;
    *)
        echo "Unknown action, branch or build: ${CEPH_BASE_BRANCH}~${DISTRO_KIND}" >&2
        exit 2
    ;;
esac
