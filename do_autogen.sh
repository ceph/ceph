#!/bin/bash

usage() {
    cat <<EOF
do_autogen.sh: make a ceph build by running autogen, etc.

-h:                              this help message
-d <level>                       debug build
                                 level 0: no debug
                                 level 1: -g
                                 level 3: -Wextra
                                 level 4: even more...
-H                               --with-hadoop
-T                               --without-tcmalloc
-e <path>                        dump encoded objects to <path>
-P                               profiling build
-p                               google profiler
-O <level>                       optimize
-n                               use libnss
-j                               with java

EOF
}

die() {
    echo $@
    exit 1
}

debug_level=0
verbose=0
profile=0
CONFIGURE_FLAGS=""
while getopts  "d:e:hHTPjpnvO:" flag
do
    case $flag in
    d) debug_level=$OPTARG;;

    O) CFLAGS="${CFLAGS} -O$OPTARG";;

    n) CONFIGURE_FLAGS="$CONFIGURE_FLAGS --with-nss --without-cryptopp";;

    P) profile=1;;
    p) with_profiler="--with-profiler" ;;

    h) usage
        exit 0;;

    H) CONFIGURE_FLAGS="$CONFIGURE_FLAGS --with-hadoop";;

    T) CONFIGURE_FLAGS="$CONFIGURE_FLAGS --without-tcmalloc";;

    j) CONFIGURE_FLAGS="$CONFIGURE_FLAGS --enable-cephfs-java";;

    v) verbose=1;;

    e) encode_dump=$OPTARG;;

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
    CFLAGS="${CFLAGS} -fno-omit-frame-pointer -O2"
    CXXFLAGS="${CXXFLAGS} -fno-omit-frame-pointer -O2"
    debug_level=1
fi

if [ "${debug_level}" -ge 1 ]; then
    CFLAGS="${CFLAGS} -g"
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

if [ -n "${encode_dump}" ]; then
    CXXFLAGS="${CXXFLAGS} -DENCODE_DUMP=${encode_dump}"
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
--with-debug $with_profiler --with-cryptopp --with-radosgw \
$CONFIGURE_FLAGS \
|| die "configure failed"
