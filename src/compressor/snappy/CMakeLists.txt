# snappy

set(snappy_sources
  CompressionPluginSnappy.cc
)

add_library(ceph_snappy SHARED ${snappy_sources})
add_dependencies(ceph_snappy ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_include_directories(ceph_snappy SYSTEM PRIVATE
  "${SNAPPY_INCLUDE_DIR}")
target_link_libraries(ceph_snappy ${SNAPPY_LIBRARIES})
set_target_properties(ceph_snappy PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_snappy DESTINATION ${compressor_plugin_dir})

if(WITH_EMBEDDED)
  add_library(cephd_compressor_snappy STATIC ${snappy_sources})
  set_target_properties(cephd_compressor_snappy PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
endif()
