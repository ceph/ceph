
set(compressor_srcs 
  Compressor.cc
  AsyncCompressor.cc)
add_library(compressor_objs OBJECT ${compressor_srcs})

## compressor plugins

set(compressor_plugin_dir ${CMAKE_INSTALL_PKGLIBDIR}/compressor)

add_subdirectory(snappy)
add_subdirectory(zlib)
add_subdirectory(zstd)

if (HAVE_LZ4)
  add_subdirectory(lz4)
endif()

set(ceph_compressor_libs
    ceph_snappy
    ceph_zlib
    ceph_zstd)

if (HAVE_LZ4)
  list(APPEND ceph_compressor_libs ceph_lz4)
endif()

add_custom_target(compressor_plugins DEPENDS
    ${ceph_compressor_libs})
