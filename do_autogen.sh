#!/bin/bash

usage() {
    cat <<EOF
do_autogen.sh: make a ceph build by running autogen, etc.

-C <parameter>                   add parameters to configure
-c                               use cryptopp
-d <level>                       debug build
                                 level 0: no debug
                                 level 1: -g
                                 level 3: -Wextra
                                 level 4: even more...
-e <path>                        dump encoded objects to <path>
-h                               this help message
-j                               with java
-J				 --with-jemalloc
-L				 --without-lttng
-O <level>                       optimize
-p                               google profiler
-P                               profiling build
-R                               without rocksdb
-S                               --without-hardening
-T                               --without-tcmalloc
-v                               verbose output

EOF
}

die() {
    echo $@
    exit 1
}

debug_level=0
verbose=0
profile=0
rocksdb=1
CONFIGURE_FLAGS="--disable-static --with-lttng"
while getopts  "C:cd:e:hjJLO:pPRSTv" flag
do
    case $flag in
    C) CONFIGURE_FLAGS="$CONFIGURE_FLAGS $OPTARG";;
    c) CONFIGURE_FLAGS="$CONFIGURE_FLAGS --with-cryptopp --without-nss";;
    d) debug_level=$OPTARG;;
    e) encode_dump=$OPTARG;;
    h) usage ; exit 0;;
    j) CONFIGURE_FLAGS="$CONFIGURE_FLAGS --enable-cephfs-java";;
    J) CONFIGURE_FLAGS="$CONFIGURE_FLAGS --with-jemalloc";;
    L) CONFIGURE_FLAGS="$CONFIGURE_FLAGS --without-lttng";;
    O) if [ $OPTARG -lt 2 ]; then
           CONFIGURE_FLAGS="$CONFIGURE_FLAGS --without-hardening"
       fi
       CFLAGS="${CFLAGS} -O$OPTARG";;
    p) with_profiler="--with-profiler" ;;
    P) profile=1;;
    R) rocksdb=0;;
    S) CONFIGURE_FLAGS="$CONFIGURE_FLAGS --without-hardening";;
    T) CONFIGURE_FLAGS="$CONFIGURE_FLAGS --without-tcmalloc";;
    v) verbose=1;;

    *) echo ; usage ; exit 1;;
    esac
done

if [ $rocksdb -eq 1 ]; then
    CONFIGURE_FLAGS="$CONFIGURE_FLAGS --with-librocksdb-static"
fi

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
    if [ "${CXX}" != "clang++" ]; then
        CXXFLAGS="${CXXFLAGS} -Wstrict-null-sentinel -Woverloaded-virtual"
    else
        CXXFLAGS="${CXXFLAGS} -Woverloaded-virtual"
    fi
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
--with-debug $with_profiler --with-nss --without-cryptopp --with-radosgw \
$CONFIGURE_FLAGS \
|| die "configure failed"
