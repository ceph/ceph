#!/bin/bash

usage() {
    cat <<EOF
do_autogen.sh: make a ceph build by running autogen, etc.

-h:                              this help message
-d <level>                       debug build
                                 level 0: no debug
                                 level 1: -g
                                 level 2: -Wall
                                 level 3: -Wextra
                                 level 4: even more...
-P                               profiling build

EOF
}

die() {
    echo $@
    exit 1
}

debug_level=0
verbose=0
CFLAGS=""
CXXFLAGS=""
profile=0
while getopts  "36d:hPv" flag
do
    case $flag in
    d) debug_level=$OPTARG;;

    P) profile=1;;

    h) usage
        exit 0;;

    v) verbose=1;;

    *)
        echo
        usage
        exit 1;;
    esac
done

if [ $profile -eq 1 ]; then
    if [ $debug_level -ne 0 ]; then
       echo "Can't specify both -d and -P. Profiling builds are \
different than debug builds."
       exit 1
    fi
    CFLAGS="${CFLAGS} -fno-omit-frame-pointer"
    debug_level=1
fi

if [ "${debug_level}" -ge 1 ]; then
    CFLAGS="${CFLAGS} -g"
fi
if [ "${debug_level}" -ge 2 ]; then
    CFLAGS="${CFLAGS} -Wall -Wvolatile-register-var"
fi
if [ "${debug_level}" -ge 3 ]; then
    CFLAGS="${CFLAGS} -Wextra \
-Wno-missing-field-initializers -Wno-missing-declarations"
fi
if [ "${debug_level}" -ge 4 ]; then
    CXXFLAGS="${CXXFLAGS} -Wstrict-null-sentinel -Woverloaded-virtual"
    CFLAGS="${CFLAGS} \
-Wuninitialized -Winit-self \
-Wformat=2 -Wunused -Wfloat-equal \
-Wundef -Wunsafe-loop-optimizations -Wpointer-arith -Wcast-qual \
-Wcast-align -Wwrite-strings -Wlogical-op \
-Wmissing-format-attribute -Wredundant-decls -Winvalid-pch"
fi

if [ "${debug_level}" -ge 5 ]; then
    CFLAGS="${CFLAGS} -Wswitch-enum -Wpacked"
fi
if [ "${debug_level}" -ge 2000 ]; then
    CFLAGS="${CFLAGS} -Wsign-promo -Wconversion -Waggregate-return -Wlong-long"
    CXXFLAGS="${CXXFLAGS} -Wold-style-cast"
fi

# Warning about unused parameters just leads to a lot of pointless spew when
# using C++. It doesn't interact well with class inheritance.
CFLAGS="${CFLAGS} -Wno-unused-parameter"

CXXFLAGS="${CXXFLAGS} ${CFLAGS}"

if [ "${verbose}" -ge 1 ]; then
    echo "CFLAGS=${CFLAGS}"
    echo "CXXFLAGS=${CFLAGS}"
fi

export CFLAGS
export CXXFLAGS

./autogen.sh || die "autogen failed"

./configure \
--prefix=/usr --sbindir=/sbin --localstatedir=/var --sysconfdir=/etc \
--with-gtk2=yes --with-debug $with_profiler \
|| die "configure failed"
