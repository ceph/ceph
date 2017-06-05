# qatzip

set(qatzip_sources
  CompressionPluginQatzip.cc
)

add_library(ceph_qatzip SHARED ${qatzip_sources})
add_dependencies(ceph_qatzip ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_qatzip ${QATZIP_LIBRARIES})
set_target_properties(ceph_qatzip PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_qatzip DESTINATION ${compressor_plugin_dir})

if(WITH_EMBEDDED)
  add_library(cephd_compressor_qatzip STATIC ${snappy_sources})
  set_target_properties(cephd_compressor_qatzip PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
endif()
