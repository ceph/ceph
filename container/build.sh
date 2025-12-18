#!/bin/bash -ex
# vim: ts=4 sw=4 expandtab

# repo auth with write perms must be present (this script does not log into
# repos named by CONTAINER_REPO_*).
# If NO_PUSH is set, no login is necessary
# If REMOVE_LOCAL_IMAGES is true (the default), local images are removed
# after push.  If you want to save local image copies, set this to false.


CFILE=${1:-Containerfile}
shift || true

usage() {
    cat << EOF
$0 [containerfile] (defaults to 'Containerfile')
For a CI build (from ceph-ci.git, built and pushed to shaman):
CI_CONTAINER: must be 'true'
FROM_IMAGE: defaults to quay.io/centos/centos9:stream
FLAVOR (OSD flavor, default or crimson)
BRANCH (of Ceph. <remote>/<ref>)
CEPH_SHA1 (of Ceph)
ARCH (of build host, and resulting container)
CONTAINER_REPO_HOSTNAME (quay.ceph.io, for CI, for instance)
CONTAINER_REPO_ORGANIZATION (ceph-ci, for CI, for instance)
CONTAINER_REPO (ceph, for CI, or prerelease-<arch> for release, for instance)
CONTAINER_REPO_USERNAME
CONTAINER_REPO_PASSWORD
PRERELEASE_USERNAME for download.ceph.com:/prerelease/ceph
PRERELEASE_PASSWORD
REMOVE_LOCAL_IMAGES set to 'false' if you want to keep local images

For a release build: (from ceph.git, built and pushed to download.ceph.com)
CI_CONTAINER: must be 'false'
and you must also add
VERSION (for instance, 19.1.0) for tagging the image
REMOVE_LOCAL_IMAGES set to 'false' if you want to keep local images

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
REPO_ARCH=amd64
if [[ "${ARCH}" = arm64 ]] ; then
    REPO_ARCH=arm64
fi
REMOVE_LOCAL_IMAGES=${REMOVE_LOCAL_IMAGES:-true}

if [[ ${CI_CONTAINER} == "true" ]] ; then
    CONTAINER_REPO_HOSTNAME=${CONTAINER_REPO_HOSTNAME:-quay.ceph.io}
    CONTAINER_REPO_ORGANIZATION=${CONTAINER_REPO_ORGANIZATION:-ceph-ci}
    CONTAINER_REPO=${CONTAINER_REPO:-ceph}
else
    CONTAINER_REPO_HOSTNAME=${CONTAINER_REPO_HOSTNAME:-quay.ceph.io}
    CONTAINER_REPO_ORGANIZATION=${CONTAINER_REPO_ORGANIZATION:-ceph}
    CONTAINER_REPO=${CONTAINER_REPO:-prerelease-${REPO_ARCH}}
    # default: most-recent annotated tag
    VERSION=${VERSION:-$(git describe --abbrev=0)}
fi

# check for existence of all required variables
: "${CI_CONTAINER:?}"
: "${FLAVOR:?}"
: "${BRANCH:?}"
: "${CEPH_SHA1:?}"
: "${ARCH:?}"
: "${REMOVE_LOCAL_IMAGES:?}"
if [[ ${NO_PUSH} != "true" ]] ; then
    : "${CONTAINER_REPO_HOSTNAME:?}"
    : "${CONTAINER_REPO_ORGANIZATION:?}"
    : "${CONTAINER_REPO_USERNAME:?}"
    : "${CONTAINER_REPO_PASSWORD:?}"
fi
if [[ ${CI_CONTAINER} != "true" ]] ; then : "${VERSION:?}"; fi

# set container engine
CONTAINER_ENGINE=${CONTAINER_ENGINE:-podman}
if [[ ${CONTAINER_ENGINE} != "podman" && ${CONTAINER_ENGINE} != "docker" ]]; then
    echo "CONTAINER_ENGINE must be 'podman' or 'docker', current: ${CONTAINER_ENGINE}"
    exit 1
fi

# check for valid repo auth (if pushing)
repopath=${CONTAINER_REPO_HOSTNAME}/${CONTAINER_REPO_ORGANIZATION}/${CONTAINER_REPO}
MINIMAL_IMAGE=${repopath}:minimal-test
if [[ ${NO_PUSH} != "true" ]] ; then
    ${CONTAINER_ENGINE} rmi ${MINIMAL_IMAGE} || true
    echo "FROM scratch" | ${CONTAINER_ENGINE} build -f - -t ${MINIMAL_IMAGE}
    if ! ${CONTAINER_ENGINE} push ${MINIMAL_IMAGE} ; then
        echo "Not authenticated to ${repopath}; need docker/podman login?"
        exit 1
    fi
    ${CONTAINER_ENGINE} rmi ${MINIMAL_IMAGE} | true
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

# podman/docker build only supports secret files.
# This must be removed after podman/docker build
touch prerelease.secret.txt
chmod 600 prerelease.secret.txt
echo -e "\
    PRERELEASE_USERNAME=${PRERELEASE_USERNAME}\n
    PRERELEASE_PASSWORD=${PRERELEASE_PASSWORD}\n " > prerelease.secret.txt

CONTAINER_BUILD_ARGS=(
  --squash
  -f "$CFILE"
  -t build.sh.output
  --build-arg FROM_IMAGE="${FROM_IMAGE:-quay.io/centos/centos:stream9}"
  --build-arg CEPH_SHA1="${CEPH_SHA1}"
  --build-arg CEPH_GIT_REPO="${CEPH_GIT_REPO}"
  --build-arg CEPH_REF="${BRANCH:-main}"
  --build-arg OSD_FLAVOR="${FLAVOR:-default}"
  --build-arg CI_CONTAINER="${CI_CONTAINER:-default}"
  --build-arg CUSTOM_CEPH_REPO_URL="${CUSTOM_CEPH_REPO_URL}"
  "--secret=id=prerelease_creds,src=./prerelease.secret.txt"
)

if [[ ${CONTAINER_ENGINE} == "podman" ]]; then
    CMD=("${CONTAINER_ENGINE}" build --pull=newer "${CONTAINER_BUILD_ARGS[@]}")
    echo "+ ${CMD[*]}"
    "${CMD[@]}" 2>&1
else
    CMD=("${CONTAINER_ENGINE}" build --pull "${CONTAINER_BUILD_ARGS[@]}" .)
    echo "+ DOCKER_BUILDKIT=1 ${CMD[*]}"
    DOCKER_BUILDKIT=1 "${CMD[@]}" 2>&1
fi

rm ./prerelease.secret.txt

# get image id
image_id=$(${CONTAINER_ENGINE} image ls build.sh.output --format '{{.ID}}')
if [[ -z "${image_id}" ]]; then
    image_id=$(${CONTAINER_ENGINE} image ls localhost/build.sh.output --format '{{.ID}}' || true)
fi
if [[ -z "${image_id}" ]]; then
    echo "ERROR: build.sh.output image not found!"
    exit 1
fi

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
vars="$(${CONTAINER_ENGINE} inspect -f '{{printf "export CEPH_CONTAINER_ARCH=%v" .Architecture}}' ${image_id})
$(${CONTAINER_ENGINE} inspect -f '{{range $index, $value := .Config.Env}}export CEPH_CONTAINER_{{$value}}{{println}}{{end}}' ${image_id})"
vars="$(echo "${vars}" | grep -v PATH)"
eval ${vars}

# remove everything up to and including the last slash
fromtag=${CEPH_CONTAINER_FROM_IMAGE##*/}
# translate : to -
fromtag=${fromtag/:/-}
builddate=$(date -u +%Y%m%d)
local_tag=${fromtag}-${CEPH_CONTAINER_CEPH_REF}-${CEPH_CONTAINER_ARCH}-${builddate}

repopath=${CONTAINER_REPO_HOSTNAME}/${CONTAINER_REPO_ORGANIZATION}/${CONTAINER_REPO}

if [[ ${CI_CONTAINER} == "true" ]] ; then
    # ceph-ci conventions for remote tags:
    # requires ARCH, BRANCH, CEPH_SHA1, FLAVOR
    full_repo_tag=${repopath}:${BRANCH}-${fromtag}-${ARCH}-devel
    branch_repo_tag=${repopath}:${BRANCH}
    sha1_repo_tag=${repopath}:${CEPH_SHA1}

    # while we have more than just centos9 containers:
    # anything that's not gets suffixed with its fromtag
    # for the branch and sha1 tags (for example, <branch>-rocky-10).
    # The default can change when it needs to.

    if [[ "${fromtag}" != "centos-stream9" ]] ; then
        branch_repo_tag=${repopath}:${BRANCH}-${fromtag}
        sha1_repo_tag=${repopath}:${CEPH_SHA1}-${fromtag}
    fi

    if [[ "${ARCH}" == "arm64" ]] ; then
        branch_repo_tag=${branch_repo_tag}-arm64
        sha1_repo_tag=${sha1_repo_tag}-arm64
    fi

    ${CONTAINER_ENGINE} tag ${image_id} ${full_repo_tag}
    ${CONTAINER_ENGINE} tag ${image_id} ${branch_repo_tag}
    ${CONTAINER_ENGINE} tag ${image_id} ${sha1_repo_tag}

    if [[ (${FLAVOR} == "crimson-debug" || ${FLAVOR} == "crimson-release") && ${ARCH} == "x86_64" ]] ; then
        sha1_flavor_repo_tag=${sha1_repo_tag}-${FLAVOR}
        ${CONTAINER_ENGINE} tag ${image_id} ${sha1_flavor_repo_tag}
        if [[ -z "${NO_PUSH}" ]] ; then
            ${CONTAINER_ENGINE} push ${sha1_flavor_repo_tag}
            if [[ ${REMOVE_LOCAL_IMAGES} == "true" ]] ; then
                ${CONTAINER_ENGINE} rmi -f ${sha1_flavor_repo_tag}
            fi
        fi
        exit
    fi

    if [[ -z "${NO_PUSH}" ]] ; then
        ${CONTAINER_ENGINE} push ${full_repo_tag}
        ${CONTAINER_ENGINE} push ${branch_repo_tag}
        ${CONTAINER_ENGINE} push ${sha1_repo_tag}
        if [[ ${REMOVE_LOCAL_IMAGES} == "true" ]] ; then
            ${CONTAINER_ENGINE} rmi -f ${full_repo_tag} ${branch_repo_tag} ${sha1_repo_tag}
        fi
    fi
else
    #
    # non-CI build.  Tags are like v19.1.0-20240701
    # push to quay.ceph.io/ceph/prerelease-$REPO_ARCH
    #
    version_tag=${repopath}:v${VERSION}-${builddate}

    ${CONTAINER_ENGINE} tag ${image_id} ${version_tag}
    if [[ -z "${NO_PUSH}" ]] ; then
        ${CONTAINER_ENGINE} push ${version_tag}
        if [[ ${REMOVE_LOCAL_IMAGES} == "true" ]] ; then
            ${CONTAINER_ENGINE} rmi -f ${version_tag}
        fi
    fi
fi