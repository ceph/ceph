# zlib

set(zlib_sources
  CompressionPluginZlib.cc
  CompressionZlib.cc
)

add_library(ceph_zlib SHARED ${zlib_sources})
add_dependencies(ceph_zlib ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_zlib z)
set_target_properties(ceph_zlib PROPERTIES VERSION 2.0.0 SOVERSION 2)
install(TARGETS ceph_zlib DESTINATION lib/compressor)
