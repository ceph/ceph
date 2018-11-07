# zstdmt

# libzstd - build it statically
set(ZSTD_C_FLAGS "-fPIC -Wno-unused-variable -O3")
include(ExternalProject)
ExternalProject_Add(zstd_ex
            SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/zstd/build/cmake
            CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
            -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
            -DCMAKE_C_FLAGS=${ZSTD_C_FLAGS}
            -DCMAKE_AR=${CMAKE_AR}
            BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/libzstd
            BUILD_COMMAND $(MAKE) libzstd_static
            INSTALL_COMMAND "true")
# force zstd make to be called on each time
ExternalProject_Add_Step(zstd_ex forcebuild
            DEPENDEES configure
            DEPENDERS build
            COMMAND "true"
            ALWAYS 1)
add_library(zstd STATIC IMPORTED)
set_target_properties(zstd PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src/zstd/lib"
            IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/libzstd/lib/libzstd.a")
add_dependencies(zstd zstd_ex)
include_directories(${CMAKE_CURRENT_LIST_DIR}/lib ${CMAKE_CURRENT_LIST_DIR}/libzstdmt)
set(zstdmt_sources
  CompressionPluginZstdMt.cc
  ${CMAKE_CURRENT_LIST_DIR}/lib/zstd-mt_common.c
  ${CMAKE_CURRENT_LIST_DIR}/lib/zstd-mt_compress.c
  ${CMAKE_CURRENT_LIST_DIR}/lib/zstd-mt_decompress.c
  ${CMAKE_CURRENT_LIST_DIR}/lib/threading.c)
add_library(ceph_zstdmt SHARED ${zstdmt_sources})
target_link_libraries(ceph_zstdmt PRIVATE zstd)
install(TARGETS ceph_zstdmt DESTINATION ${compressor_plugin_dir})
