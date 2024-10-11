#!/bin/bash -ex
# vim: ts=4 sw=4 expandtab

# repo auth with write perms must be present (this script does not log into
# CONTAINER_REPO_HOSTNAME and CONTAINER_REPO_ORGANIZATION).
# If NO_PUSH is set, no login is necessary


CFILE=${1:-Containerfile}
shift || true

usage() {
    cat << EOF
$0 [containerfile] (defaults to 'Containerfile')
For a CI build (from ceph-ci.git, built and pushed to shaman):
CI_CONTAINER: must be 'true'
FLAVOR (OSD flavor, default or crimson)
BRANCH (of Ceph. <remote>/<ref>)
CEPH_SHA1 (of Ceph)
ARCH (of build host, and resulting container)
CONTAINER_REPO_HOSTNAME (quay.ceph.io, for CI, for instance)
CONTAINER_REPO_ORGANIZATION (ceph-ci, for CI, for instance)
CONTAINER_REPO_USERNAME
CONTAINER_REPO_PASSWORD

For a release build: (from ceph.git, built and pushed to download.ceph.com)
CI_CONTAINER: must be 'false'
and you must also add
VERSION (for instance, 19.1.0) for tagging the image

You can avoid the push step (for testing) by setting NO_PUSH to anything
EOF
}

CI_CONTAINER=${CI_CONTAINER:-false}
FLAVOR=${FLAVOR:-default}
# default: current checked-out branch
BRANCH=${BRANCH:-$(git rev-parse --abbrev-ref HEAD)}
# default: current checked-out branch
CEPH_SHA1=${CEPH_SHA1:-$(git rev-parse HEAD)}
# default: build host arch
ARCH=${ARCH:-$(arch)}
if [[ "${ARCH}" == "aarch64" ]] ; then ARCH=arm64; fi
if [[ ${CI_CONTAINER} == "true" ]] ; then
    CONTAINER_REPO_HOSTNAME=${CONTAINER_REPO_HOSTNAME:-quay.ceph.io}
    CONTAINER_REPO_ORGANIZATION=${CONTAINER_REPO_ORGANIZATION:-ceph/ceph-${ARCH}}
else
    CONTAINER_REPO_HOSTNAME=${CONTAINER_REPO_HOSTNAME:-quay.io}
    CONTAINER_REPO_ORGANIZATION=${CONTAINER_REPO_ORGANIZATION:-ceph/ceph}
    # default: most-recent annotated tag
    VERSION=${VERSION:-$(git describe --abbrev=0)}
fi

# check for existence of all required variables
: "${CI_CONTAINER:?}"
: "${FLAVOR:?}"
: "${BRANCH:?}"
: "${CEPH_SHA1:?}"
: "${ARCH:?}"
: "${CONTAINER_REPO_HOSTNAME:?}"
: "${CONTAINER_REPO_ORGANIZATION:?}"
: "${CONTAINER_REPO_USERNAME:?}"
: "${CONTAINER_REPO_PASSWORD:?}"
if [[ ${CI_CONTAINER} != "true" ]] ; then ${VERSION:?}; fi

# check for valid repo auth (if pushing)
ORGURL=${CONTAINER_REPO_HOSTNAME}/${CONTAINER_REPO_ORGANIZATION}
MINIMAL_IMAGE=${ORGURL}/ceph:minimal-test
if [[ ${NO_PUSH} != "true" ]] ; then
    podman rmi ${MINIMAL_IMAGE} || true
    echo "FROM scratch" | podman build -f - -t ${MINIMAL_IMAGE}
    if ! podman push ${MINIMAL_IMAGE} ; then
        echo "Not authenticated to ${ORGURL}; need docker/podman login?"
        exit 1
    fi
    podman rmi ${MINIMAL_IMAGE} | true
fi

if [[ -z "${CEPH_GIT_REPO}" ]] ; then
    if [[ ${CI_CONTAINER} == "true" ]]; then
        CEPH_GIT_REPO=https://github.com/ceph/ceph-ci.git
    else
        CEPH_GIT_REPO=https://github.com/ceph/ceph.git
    fi
fi

# BRANCH will be, say, origin/main.  remove <remote>/
BRANCH=${BRANCH##*/}

podman build --pull=newer --squash -f $CFILE -t build.sh.output \
    --build-arg FROM_IMAGE=${FROM_IMAGE:-quay.io/centos/centos:stream9} \
    --build-arg CEPH_SHA1=${CEPH_SHA1} \
    --build-arg CEPH_GIT_REPO=${CEPH_GIT_REPO} \
    --build-arg CEPH_REF=${BRANCH:-main} \
    --build-arg OSD_FLAVOR=${FLAVOR:-default} \
    --build-arg CI_CONTAINER=${CI_CONTAINER:-default} \
    2>&1 

image_id=$(podman image ls localhost/build.sh.output --format '{{.ID}}')

# grab useful image attributes for building the tag
#
# the variable settings are prefixed with "export CEPH_CONTAINER_" so that
# an eval or . can be used to put them into the environment
#
# PATH is removed from the output as it would cause problems for this
# parent script and its children
#
# notes:
#
# we want .Architecture and everything in .Config.Env
#
# printf will not accept "\n" (is this a podman bug?)
# so construct vars with two calls to podman inspect, joined by a newline,
# so that vars will get the output of the first command, newline, output
# of the second command
#
vars="$(podman inspect -f '{{printf "export CEPH_CONTAINER_ARCH=%v" .Architecture}}' ${image_id})
$(podman inspect -f '{{range $index, $value := .Config.Env}}export CEPH_CONTAINER_{{$value}}{{println}}{{end}}' ${image_id})"
vars="$(echo "${vars}" | grep -v PATH)"
eval ${vars}

# remove everything up to and including the last slash
fromtag=${CEPH_CONTAINER_FROM_IMAGE##*/}
# translate : to -
fromtag=${fromtag/:/-}
builddate=$(date +%Y%m%d)
local_tag=${fromtag}-${CEPH_CONTAINER_CEPH_REF}-${CEPH_CONTAINER_ARCH}-${builddate}

repopath=${CONTAINER_REPO_HOSTNAME}/${CONTAINER_REPO_ORGANIZATION}

if [[ ${CI_CONTAINER} == "true" ]] ; then
    # ceph-ci conventions for remote tags:
    # requires ARCH, BRANCH, CEPH_SHA1, FLAVOR
    full_repo_tag=$repopath/ceph:${BRANCH}-${fromtag}-${ARCH}-devel
    branch_repo_tag=$repopath/ceph:${BRANCH}
    sha1_repo_tag=$repopath/ceph:${CEPH_SHA1}

    if [[ "${ARCH}" == "arm64" ]] ; then
        branch_repo_tag=${branch_repo_tag}-arm64
        sha1_repo_tag=${sha1_repo_tag}-arm64
    fi

    podman tag ${image_id} ${full_repo_tag}
    podman tag ${image_id} ${branch_repo_tag}
    podman tag ${image_id} ${sha1_repo_tag}

    if [[ ${FLAVOR} == "crimson" && ${ARCH} == "x86_64" ]] ; then
        sha1_flavor_repo_tag=${sha1_repo_tag}-${FLAVOR}
        podman tag ${image_id} ${sha1_flavor_repo_tag}
        if [[ -z "${NO_PUSH}" ]] ; then
            podman push ${sha1_flavor_repo_tag}
        fi
        exit
    fi

    if [[ -z "${NO_PUSH}" ]] ; then
        podman push ${full_repo_tag}
        podman push ${branch_repo_tag}
        podman push ${sha1_repo_tag}
    fi
else
    #
    # non-CI build.  Tags are like v19.1.0-20240701
    # push to quay.ceph.io/ceph/prerelease
    #
    version_tag=${repopath}/prerelease/ceph-${ARCH}:${VERSION}-${builddate}

    podman tag ${image_id} ${version_tag}
    if [[ -z "${NO_PUSH}" ]] ; then
        podman push ${image_id} ${version_tag}
    fi
fi


