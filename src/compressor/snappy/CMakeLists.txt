# snappy

set(snappy_sources
  CompressionPluginSnappy.cc
)

add_library(ceph_snappy SHARED ${snappy_sources})
target_include_directories(ceph_snappy SYSTEM PRIVATE
  "${SNAPPY_INCLUDE_DIR}")
target_link_libraries(ceph_snappy ${SNAPPY_LIBRARIES})
set_target_properties(ceph_snappy PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_snappy DESTINATION ${compressor_plugin_dir})
