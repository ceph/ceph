#!/bin/bash
#
# Build ceph in a container, creating the container environment if necessary.
# Use a build recipe (-r) to automatically perform a build step. Otherwise,
# pass CLI args after -- terminator to run whatever command you want in
# the build container.
#

set -e -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${0}")" && pwd)"
CEPH_ROOT="$SCRIPT_DIR/../.."

DISTRO=centos8
TAG=
CONTAINER_NAME="ceph-build"
BUILD_CTR=yes
BUILD=yes
BUILD_DIR=build
HOMEDIR=/build
DNF_CACHE=
ENABLE_CCACHE=volume
EXTRA_ARGS=()
BUILD_ARGS=()

show_help() {
    echo "build-in-container.sh:"
    echo "    --help                Show help"
    grep "###:" "${0}" | grep -v sed | sed 's,.*###: ,    ,'
    echo ""
}

show_recipes() {
    echo "build-in-container.sh recipes:"
    echo "  You can execute one or more recipe on the same command line."
    echo "  Each recipe will run in a new container instance."
    echo ""
    grep "###%" "${0}" | grep -v sed | sed 's,.*###% ,    ,'
    echo ""
}

get_engine() {
    if command -v podman >/dev/null 2>&1; then
        echo podman
        return 0
    fi
    if command -v docker >/dev/null 2>&1; then
        echo docker
        return 0
    fi
    echo "ERROR: no container engine found" >&2
    return 2
}

build_container() {
    engine="$(get_engine)"
    cmd=("${engine}" build -t "${CONTAINER_NAME}:${TAG}" --build-arg JENKINS_HOME="$HOMEDIR")
    if [ "$DISTRO" ]; then
        cmd+=(--build-arg DISTRO="${DISTRO}")
    fi
    if [ "$DNF_CACHE" ]; then
        mkdir -p "$DNF_CACHE/lib" "$DNF_CACHE/cache"
        cmd+=(-v "$DNF_CACHE/lib:/var/lib/dnf:Z"
              -v "$DNF_CACHE/cache:/var/cache/dnf:Z"
              --build-arg CLEAN_DNF=no)
    fi
    cmd+=(-f Dockerfile.build .)

    "${cmd[@]}"
}

# get_recipe extends BUILD_ARGS with commands to execute the named recipe.
# Exits on an invalid recipe.
get_recipe() {
    RECIPE_BUILD_ARGS=()
    case "$1" in
        ###% configure
        ###%     Runs the configure function with defaults for unit testing.
        configure)
            RECIPE_BUILD_ARGS=(bash -c 'cd /build && source ./src/script/run-make.sh && configure')
        ;;
        ###% configure-if-needed
        ###%     Runs the configure recipe only when the build dir doesn't exist.
        configure-if-needed)
            RECIPE_BUILD_ARGS=(bash -c 'cd /build && source ./src/script/run-make.sh && has_build_dir || configure')
        ;;
        ###% build
        ###%     Runs the build (without support for unit tests).
        build)
            RECIPE_BUILD_ARGS=(bash -c 'cd /build && source ./src/script/run-make.sh && enable_compiler_env && build vstart')
        ;;
        ###% build-tests
        ###%     Runs the build with support for unit tests.
        build-tests)
            RECIPE_BUILD_ARGS=(bash -c 'cd /build && source ./src/script/run-make.sh && enable_compiler_env && build tests')
        ;;
        ###% run-tests
        ###%     Runs the unit test suite.
        run-tests)
            # shellcheck disable=SC2016 # We want the bash-in-container to expand the var
            RECIPE_BUILD_ARGS=(bash -c 'cd /build && source ./run-make-check.sh && enable_compiler_env && cd $BUILD_DIR && run')
        ;;
        ###% make-srpm
        ###%     Generates a source rpm (assumes a CentOS/RH container).
        make-srpm)
            RECIPE_BUILD_ARGS=(bash -c 'cd /build && ./make-srpm.sh')
        ;;
        ###% rpmbuild
        ###%     Builds binary rpms from source rpm (assumes a CentOS/RH container).
        rpmbuild)
            RECIPE_BUILD_ARGS=(bash -c 'rpmbuild --rebuild -D"_topdir /build/_rpm" /build/ceph-*.src.rpm')
        ;;
        *)
            echo "invalid recipe: $1" >&2
            exit 2
        ;;
    esac
}

build_ceph() {
    engine="$(get_engine)"
    cmd=("${engine}" run --name ceph_build --rm)
    case "$engine" in
        *podman)
            cmd+=("--pids-limit=-1")
        ;;
    esac
    local hname="${TAG:=ceph-build}"
    cmd+=(--hostname="${hname//./-}")
    cmd+=(-v "$PWD:$HOMEDIR:Z")
    cmd+=(-e "HOMEDIR=$HOMEDIR")
    cmd+=(-e "BUILD_DIR=$BUILD_DIR")

    if [ "${ENABLE_CCACHE}" = "volume" ]; then
        ccvol="ceph-ccache-${TAG:=default}"
        if ! "${engine}" volume exists "${ccvol}" ; then
            "${engine}" volume create "${ccvol}"
        fi
        cmd+=(-v "$ccvol:$HOMEDIR/.ccache:Z")
        cmd+=(-e "CCACHE_DIR=$HOMEDIR/.ccache")
        cmd+=(-e "CCACHE_BASEDIR=${HOMEDIR}")
    fi
    cmd+=("${EXTRA_ARGS[@]}")
    cmd+=("$CONTAINER_NAME:$TAG")

    "${cmd[@]}" "$@"
}

parse_cli() {
    CLI="$(getopt -o hd:t:x:b:r: --long help,distro:,tag:,name:,no-build,no-container-build,dnf-cache:,build-dir:,recipe:,help-recipes -n "$0" -- "$@")"
    eval set -- "${CLI}"
    while true ; do
        case "$1" in
            ###: -d / --distro=<VALUE>
            ###:     Specify a distro image or short name (eg. centos8)
            -d|--distro)
                DISTRO="$2"
                shift
                shift
            ;;
            ###: -t / --tag=<VALUE>
            ###:     Specify a container tag (eg. main)
            -t|--tag)
                TAG="$2"
                shift
                shift
            ;;
            ###: --name=<VALUE>
            ###:     Specify a container name (default: ceph-build)
            --name)
                CONTAINER_NAME="$2"
                shift
                shift
            ;;
            ###: --dnf-cache=<VALUE>
            ###:     Enable dnf caching in given dir (build container)
            --dnf-cache)
                DNF_CACHE="$2"
                shift
                shift
            ;;
            ###: -b / --build-dir=<VALUE>
            ###:     Specify (relative) build directory to use (ceph build)
            -b|--build-dir)
                BUILD_DIR="$2"
                shift
                shift
            ;;
            ###: -x<ARG>
            ###:     Pass extra argument to container run command
            -x)
                EXTRA_ARGS+=("$2")
                shift
                shift
            ;;
            ###: -r / --recipe=<VALUE>
            ###:     Ceph build recipe to use. If not provided, the remaining
            ###:     arguments will be executed directly as a build commmand.
            ###:     Can be specified more than once. Use --help-recipes for details.
            -r|--recipe)
                BUILD_RECIPES+=("$2")
                shift
                shift
            ;;
            ###: --no-build
            ###:     Skip building Ceph
            --no-build)
                BUILD=no
                shift
            ;;
            ###: --no-container-build
            ###:     Skip constructing a build container
            --no-container-build)
                BUILD_CTR=no
                shift
            ;;
            ###: --help-recipes
            ###:     Show available build recipes.
            --help-recipes)
                show_recipes
                exit 0
            ;;
            -h|--help)
                show_help
                exit 0
            ;;
            --)
                shift
                BUILD_ARGS+=("$@")
                break
            ;;
            *)
                echo "unknown option: $1" >&2
                exit 2
            ;;
        esac
    done
}

build_in_container() {
    parse_cli "$@"
    # We didn't want tracing before, it is all boring boilerplate and cli
    # handling. We enable it now that we're beginning to run interesting
    # stuff and want to know what we are actually executing.
    set -x

    case "$DISTRO" in
        ubuntu22.04)
            DISTRO_NAME="ubuntu22.04"
            DISTRO="docker.io/ubuntu:22.04"
        ;;
        centos9|centos?stream9)
            DISTRO_NAME="centos9"
            DISTRO="quay.io/centos/centos:stream9"
        ;;
        centos8|centos?stream8)
            DISTRO_NAME="centos8"
            DISTRO="quay.io/centos/centos:stream8"
        ;;
        *)
            DISTRO_NAME="custom"
        ;;
    esac

    if [ -z "${TAG}" ]; then
        GIT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
        TAG="${GIT_BRANCH}.${DISTRO_NAME}"
    fi

    cd "$CEPH_ROOT"
    if [ "$BUILD_CTR" = yes ]; then
        build_container
    fi
    if [ "$BUILD" = yes ]; then
        for rec in "${BUILD_RECIPES[@]}"; do
            get_recipe "${rec}"
            build_ceph "${RECIPE_BUILD_ARGS[@]}"
        done
        if [ "${#BUILD_ARGS[@]}" -gt 0 ]; then
            build_ceph "${BUILD_ARGS[@]}"
        fi
    fi
}

if [ "$0" = "${BASH_SOURCE[0]}" ]; then
    build_in_container "$@"
fi
