# Set the FreeBSD specific configure flags
FREEBSD_CONFIGURE_FLAGS=
if [ "$(uname)" = FreeBSD ]; then
    CC=clang
    CXX=clang++
    #CC=gcc48
    #CXX=g++48
    CLANGWARN="-Wno-unused-local-typedef -Wno-mismatched-tags -Wno-macro-redefined -Wno-unused-function -Wno-unused-label -Wno-undefined-bool-conversion -Wno-unused-private-field -Wno-unused-local-typedef -Wno-uninitialized -Wno-gnu-designator -Wno-inconsistent-missing-override -Wno-deprecated-declarations"
    CFLAGS="-I/usr/local/include ${CLANGWARN}"
    CXXFLAGS="-DGTEST_USE_OWN_TR1_TUPLE -I/usr/local/include ${CLANGWARN}"
    LDFLAGS="${LDFLAGS} -L/usr/local/lib -export-dynamic -luuid"
    FREEBSD_CONFIGURE_FLAGS="
      --disable-silent-rules
      --with-debug
      --with-rados
      --with-rbd 
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
      --with-libzfs=no
      --without-fuse
      --without-lttng
        "
#      --with-cephfs
#      --without-radosgw
#      --with-gnu-ld
fi

CONFIGURE_FLAGS="${FREEBSD_CONFIGURE_FLAGS}"

# Export these so that ./configure will pick up 
export CC
export CXX
export CFLAGS
export CXXFLAGS
export CONFIGURE_FLAGS
export LDFLAGS
