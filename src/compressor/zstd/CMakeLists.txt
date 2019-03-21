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
             -DCMAKE_POSITION_INDEPENDENT_CODE=${ENABLE_SHARED}
             -G${CMAKE_GENERATOR}
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/libzstd
  BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target libzstd_static
  INSTALL_COMMAND "true")

# force zstd make to be called on each time
ExternalProject_Add_Step(zstd_ext forcebuild
  DEPENDEES configure
  DEPENDERS build
  COMMAND "true"
  ALWAYS 1)

add_library(zstd STATIC IMPORTED)
set_target_properties(zstd PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src/zstd/lib"
  IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/libzstd/lib/libzstd.a")
add_dependencies(zstd zstd_ext)

set(zstd_sources
  CompressionPluginZstd.cc
)

add_library(ceph_zstd SHARED ${zstd_sources})
target_link_libraries(ceph_zstd PRIVATE zstd)
set_target_properties(ceph_zstd PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_zstd DESTINATION ${compressor_plugin_dir})
