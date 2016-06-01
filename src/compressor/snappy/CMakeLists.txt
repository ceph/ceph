# snappy

set(snappy_sources
  CompressionPluginSnappy.cc
)

add_library(ceph_snappy SHARED ${snappy_sources})
add_dependencies(ceph_snappy ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_snappy snappy)
set_target_properties(ceph_snappy PROPERTIES VERSION 2.0.0 SOVERSION 2)
install(TARGETS ceph_snappy DESTINATION lib/compressor)
