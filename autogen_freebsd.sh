# Set the FreeBSD specific configure flags
FREEBSD_CONFIGURE_FLAGS=
if [ "$(uname)" = FreeBSD ]; then
    CC=clang
    CXX=clang++
    CWARN=""
    CLANGWARN="-Wno-unused-local-typedef -Wno-mismatched-tags -Wno-macro-redefined -Wno-unused-function -Wno-unused-label -Wno-undefined-bool-conversion -Wno-unused-private-field -Wno-unused-local-typedef -Wno-uninitialized -Wno-gnu-designator -Wno-inconsistent-missing-override -Wno-deprecated-declarations"
    CFLAGS="-g -I/usr/local/include ${CWARN} ${CLANGWARN}"
    CXXFLAGS="-g -DGTEST_USE_OWN_TR1_TUPLE=1 -I/usr/local/include ${CWARN} ${CLANGWARN}"
    LDFLAGS="-g ${LDFLAGS} -L/usr/local/lib -export-dynamic -luuid"
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
      --without-fuse
      --without-lttng
      --with-libzfs=no
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
