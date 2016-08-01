
set(compressor_srcs 
  Compressor.cc
  AsyncCompressor.cc)
add_library(compressor_objs OBJECT ${compressor_srcs})

## compressor plugins

set(compressor_plugin_dir ${CMAKE_INSTALL_PKGLIBDIR}/compressor)

add_subdirectory(snappy)
add_subdirectory(zlib)

add_custom_target(compressor_plugins DEPENDS
    ceph_snappy
    ceph_zlib)
