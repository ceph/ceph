# zstd

# libzstd - build it statically
set(ZSTD_EXTRA_CFLAGS -fPIC -Wno-unused-variable -O3)

add_custom_target(build_zstd
  COMMAND $(MAKE) libzstd CFLAGS="${ZSTD_EXTRA_CFLAGS}"
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/zstd/lib
  COMMENT "libzstd building")

add_library(zstd STATIC IMPORTED)
set_property(TARGET zstd PROPERTY IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/src/zstd/lib/libzstd.a")
add_dependencies(zstd build_zstd)
set(ZSTD_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/zstd/lib)

#
set(zstd_sources
  CompressionPluginZstd.cc
)

add_library(ceph_zstd SHARED ${zstd_sources})
add_dependencies(ceph_zstd ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_zstd zstd)
set_target_properties(ceph_zstd PROPERTIES VERSION 2.0.0 SOVERSION 2)
install(TARGETS ceph_zstd DESTINATION ${compressor_plugin_dir})

if(WITH_EMBEDDED)
  add_library(cephd_compressor_zstd STATIC ${zstd_sources})
  set_target_properties(cephd_compressor_zstd PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
endif()
