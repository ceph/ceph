# snappy

# absolute path because $snappy_sources used outside of this directory
set(snappy_sources
  ${CMAKE_SOURCE_DIR}/src/compressor/snappy/CompressionPluginSnappy.cc
  $<TARGET_OBJECTS:compressor_objs>
)

add_library(ceph_snappy SHARED ${snappy_sources})
add_dependencies(ceph_snappy ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_snappy ${EXTRALIBS})
set_target_properties(ceph_snappy PROPERTIES VERSION 2.14.0 SOVERSION 2)
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -lsnappy")
install(TARGETS ceph_snappy DESTINATION lib/compressor)
