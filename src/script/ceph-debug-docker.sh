#!/usr/bin/env bash

# This can be run from e.g. the senta machines which have docker available. You
# may need to run this script with sudo.
#
# Once you have booted into the image, you should be able to debug the core file:
#     $ gdb -q /ceph/teuthology-archive/.../coredump/1500013578.8678.core
#
# You may want to install other packages (yum) as desired.
#
# Once you're finished, please delete old images in a timely fashion.

set -e

CACHE=""

function run {
    printf "%s\n" "$*"
    "$@"
}

function main {
    eval set -- $(getopt --name "$0" --options 'h' --longoptions 'help,no-cache' -- "$@")

    while [ "$#" -gt 0 ]; do
        case "$1" in
            -h|--help)
                printf '%s: [--no-cache] <branch>[:sha1] <enviornment>\n' "$0"
                exit 0
                ;;
            --no-cache)
                CACHE="--no-cache"
                shift
                ;;
            --)
                shift
                break
                ;;
        esac
    done

    if [ -z "$1" ]; then
        printf "specify the branch [default \"master:latest\"]: "
        read source
        if [ -z "$source" ]; then
            source=master:latest
        fi
    else
        branch="$1"
    fi
    sha=${branch##*:}
    if [ -z "$sha" ]; then
        sha=latest
    fi
    branch=${branch%%:*}
    printf "branch: %s\nsha1: %s\n" "$branch" "$sha"

    if [ -z "$2" ]; then
        printf "specify the build environment [default \"centos:7\"]: "
        read env
        if [ -z "$env" ]; then
            env=centos:7
        fi
    else
        env="$2"
    fi
    printf "env: %s\n" "$env"

    if [ -n "$SUDO_USER" ]; then
        user="$SUDO_USER"
    elif [ -n "$USER" ]; then
        user="$USER"
    else
        user="$(whoami)"
    fi

    image="${user}/ceph-ci:${branch}:${sha}-${env/:/-}"

    T=$(mktemp -d)
    pushd "$T"
    if grep ubuntu <<<"$env" > /dev/null 2>&1; then
        # Docker makes it impossible to access anything outside the CWD : /
        cp -- /ceph/shaman/cephdev.asc .
        cat > Dockerfile <<EOF
FROM ${env}

WORKDIR /root
RUN apt-get update --yes --quiet && \
    apt-get install --yes --quiet screen wget gdb software-properties-common python-software-properties apt-transport-https
COPY cephdev.asc cephdev.asc
RUN apt-key add cephdev.asc
RUN add-apt-repository "\$(wget --quiet -O - https://shaman.ceph.com/api/repos/ceph/${branch}/${sha}/${env/://}/repo)" && \
    apt-get update --yes && \
    apt-get install --yes --allow-unauthenticated ceph ceph-osd-dbg ceph-mds-dbg ceph-mgr-dbg ceph-mon-dbg ceph-fuse-dbg ceph-test-dbg radosgw-dbg
EOF
        time run docker build $CACHE --tag "$image" .
    else # try RHEL flavor
        time run docker build $CACHE --tag "$image" - <<EOF
FROM ${env}

WORKDIR /root
RUN yum update -y && \
    yum install -y screen epel-release wget psmisc ca-certificates gdb
RUN wget -O /etc/yum.repos.d/ceph-dev.repo https://shaman.ceph.com/api/repos/ceph/${branch}/${sha}/centos/7/repo && \
    yum clean all && \
    yum upgrade -y && \
    yum install -y ceph ceph-debuginfo ceph-fuse
EOF
    fi
    popd
    rm -rf -- "$T"

    printf "built image %s\n" "$image"

    run docker run -ti -v /ceph:/ceph:ro "$image"
    return 0
}

main "$@"
