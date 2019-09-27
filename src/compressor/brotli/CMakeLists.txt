# brotli

set(brotli_sources 
  CompressionPluginBrotli.cc
  BrotliCompressor.cc
)
include(ExternalProject)
ExternalProject_Add(brotli_ext
  DOWNLOAD_DIR ${CMAKE_BINARY_DIR}/src/
  GIT_REPOSITORY "https://github.com/google/brotli.git"
  GIT_TAG "v1.0.7"
  SOURCE_DIR ${CMAKE_BINARY_DIR}/src/brotli
  CONFIGURE_COMMAND ./configure-cmake --disable-debug 
  INSTALL_COMMAND ""
  BUILD_COMMAND $(MAKE)
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND "")

set(brotli_libs enc dec common)
file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/src/brotli/c/include")
foreach(lib ${brotli_libs})
  add_library(brotli::${lib} STATIC IMPORTED)
  add_dependencies(brotli::${lib} brotli_ext)
  set_target_properties(brotli::${lib} PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}/src/brotli/c/include"
    IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/src/brotli/libbrotli${lib}-static.a")
  list(APPEND BROTLI_LIBRARIES brotli::${lib})
endforeach()

add_library(ceph_brotli SHARED ${brotli_sources})
list(REVERSE brotli_libs)
target_link_libraries(ceph_brotli PRIVATE ${BROTLI_LIBRARIES})
install(TARGETS ceph_brotli DESTINATION ${compressor_plugin_dir})
