# Set the FreeBSD specific configure flags
FREEBSD_CONFIGURE_FLAGS=
if [ x`uname`x = x"FreeBSD"x ]; then
    MAKE=gmake
    # We need at least something > clang 3.4
    # tested with package clang37 on FreeBSD 10.2 ( Which has 3.4 as default )
    if clang -v 2>&1 | grep -q "3.8" ; then
        CC=clang
        CXX=clang++
    elif [ -f /usr/local/bin/clang37 ]; then
        CC=clang37
        CXX=clang++37
    else
	echo "Need a better compiler"
	exit 1
    fi
    CWARN=""
    CLANGWARN="-Wno-unused-function -Wno-unused-local-typedef -Wno-inconsistent-missing-override"
    CLANGWARN="$CLANGWARN -Wno-unused-private-field"
    CLANGWARN="$CLANGWARN -Wno-varargs"
    CLANGWARN="$CLANGWARN -Wno-gnu-designator"

# "-Wno-unused-local-typedef -Wno-mismatched-tags -Wno-macro-redefined -Wno-unused-function -Wno-unused-label -Wno-undefined-bool-conversion -Wno-unused-private-field -Wno-unused-local-typedef -Wno-uninitialized -Wno-gnu-designator -Wno-inconsistent-missing-override -Wno-deprecated-declarations -Wno-parentheses"

    CFLAGS="-g -I/usr/local/include ${CWARN} ${CLANGWARN}"
    CXXFLAGS="-g -DGTEST_USE_OWN_TR1_TUPLE=1 -I/usr/local/include ${CWARN} ${CLANGWARN}"
    LDFLAGS="-g ${LDFLAGS} -L/usr/local/lib -export-dynamic -luuid"
    FREEBSD_CONFIGURE_FLAGS="
      --disable-silent-rules
      --disable-gitversion
      --with-debug
      --with-rados
      --without-rbd 
      --with-radosgw
      --with-radosstriper
      --with-mon
      --with-osd
      --with-mds
      --with-radosgw
      --with-nss
      --without-tcmalloc
      --without-libaio
      --without-libxfs
      --without-fuse
      --without-lttng
      --with-libzfs=no
      --without-rocksdb
      --without-cephfs
        "
#      --without-radosgw
#      --with-gnu-ld
fi

CONFIGURE_FLAGS="${FREEBSD_CONFIGURE_FLAGS}"

# Export these so that ./configure will pick up 
export MAKE
export CC
export CXX
export CFLAGS
export CXXFLAGS
export CONFIGURE_FLAGS
export LDFLAGS
