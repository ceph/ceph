# lz4

set(lz4_sources
  CompressionPluginLZ4.cc
)

add_library(ceph_lz4 SHARED ${lz4_sources})
add_dependencies(ceph_lz4 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_lz4 ${LZ4_LIBRARY})
set_target_properties(ceph_lz4 PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_lz4 DESTINATION ${compressor_plugin_dir})

if(WITH_EMBEDDED)
  add_library(cephd_compressor_lz4 STATIC ${lz4_sources})
  set_target_properties(cephd_compressor_lz4 PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
endif()
