# zstd

# libzstd - build it statically
set(ZSTD_C_FLAGS "-fPIC -Wno-unused-variable -O3")

include(ExternalProject)
ExternalProject_Add(zstd_ext
  SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/zstd/build/cmake
  CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
             -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
             -DCMAKE_C_FLAGS=${ZSTD_C_FLAGS}
             -DCMAKE_AR=${CMAKE_AR}
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/libzstd
  BUILD_COMMAND $(MAKE) libzstd_static
  INSTALL_COMMAND "true")

# force zstd make to be called on each time
ExternalProject_Add_Step(zstd_ext forcebuild
  DEPENDEES configure
  DEPENDERS build
  COMMAND "true"
  ALWAYS 1)

add_library(zstd STATIC IMPORTED)
set_property(TARGET zstd PROPERTY
  IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/libzstd/lib/libzstd.a")
add_dependencies(zstd zstd_ext)
set(ZSTD_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/zstd/lib)

#
set(zstd_sources
  CompressionPluginZstd.cc
)

add_library(ceph_zstd SHARED ${zstd_sources})
add_dependencies(ceph_zstd ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_zstd zstd)
set_target_properties(ceph_zstd PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_zstd DESTINATION ${compressor_plugin_dir})

if(WITH_EMBEDDED)
  add_library(cephd_compressor_zstd STATIC ${zstd_sources})
  set_target_properties(cephd_compressor_zstd PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
endif()
