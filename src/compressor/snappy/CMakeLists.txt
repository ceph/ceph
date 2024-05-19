# snappy

set(snappy_sources
  CompressionPluginSnappy.cc
)

add_library(ceph_snappy SHARED ${snappy_sources})
target_link_libraries(ceph_snappy
  PRIVATE
    legacy-option-headers
    snappy::snappy compressor $<$<PLATFORM_ID:Windows>:ceph-common>)
set_target_properties(ceph_snappy PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_snappy DESTINATION ${compressor_plugin_dir})
