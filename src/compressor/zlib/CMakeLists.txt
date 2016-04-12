# zlib

set(zlib_sources
  CompressionPluginZlib.cc
  CompressionZlib.cc
  $<TARGET_OBJECTS:compressor_objs>
)

add_library(ceph_zlib SHARED ${zlib_sources})
add_dependencies(ceph_zlib ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_zlib ${EXTRALIBS})
set_target_properties(ceph_zlib PROPERTIES VERSION 2.14.0 SOVERSION 2)
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -lz")
install(TARGETS ceph_zlib DESTINATION lib/compressor)
