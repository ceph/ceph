# lz4

set(lz4_sources
  CompressionPluginLZ4.cc
)

add_library(ceph_lz4 SHARED ${lz4_sources})
target_link_libraries(ceph_lz4 PRIVATE LZ4::LZ4)
set_target_properties(ceph_lz4 PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_lz4 DESTINATION ${compressor_plugin_dir})
