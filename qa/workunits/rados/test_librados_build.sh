#!/bin/bash -ex
#
# Compile and run a librados application outside of the ceph build system, so
# that we can be sure librados.h[pp] is still usable and hasn't accidentally
# started depending on internal headers.
#
# The script assumes all dependencies - e.g. curl, make, gcc, librados headers,
# libradosstriper headers, boost headers, etc. - are already installed.
#

source $(dirname $0)/../ceph-helpers-root.sh

trap cleanup EXIT

SOURCES="hello_radosstriper.cc
hello_world_c.c
hello_world.cc
Makefile
"
BINARIES_TO_RUN="hello_world_c
hello_world_cpp
"
BINARIES="${BINARIES_TO_RUN}hello_radosstriper_cpp
"
# parse output like "octopus (dev)"
case $(librados-config --release | grep -Po ' \(\K[^\)]+') in
    dev)
        BRANCH=main;;
    rc|stable)
        BRANCH=$(librados-config --release | cut -d' ' -f1);;
    *)
        echo "unknown release '$(librados-config --release)'" >&2
        return 1;;
esac
DL_PREFIX="http://git.ceph.com/?p=ceph.git;a=blob_plain;hb=${BRANCH};f=examples/librados/"
#DL_PREFIX="https://raw.githubusercontent.com/ceph/ceph/master/examples/librados/"
DESTDIR=$(pwd)

function cleanup () {
    for f in $BINARIES$SOURCES ; do
        rm -f "${DESTDIR}/$f"
    done
}

function get_sources () {
    for s in $SOURCES ; do
        curl --progress-bar --output $s ${DL_PREFIX}$s
    done
}

function check_sources () {
    for s in $SOURCES ; do
        test -f $s
    done
}

function check_binaries () {
    for b in $BINARIES ; do
        file $b
        test -f $b
    done
}

function run_binaries () {
    for b in $BINARIES_TO_RUN ; do
        ./$b -c /etc/ceph/ceph.conf
    done
}

pushd $DESTDIR
case $(distro_id) in
    centos|fedora|rhel|opensuse*|suse|sles)
        install gcc-c++ make libradospp-devel librados-devel;;
    ubuntu|debian|devuan|softiron)
        install g++ make libradospp-dev librados-dev;;
    *)
        echo "$(distro_id) is unknown, $@ will have to be installed manually."
esac
get_sources
check_sources
make all-system
check_binaries
run_binaries
popd
