# brotli

set(brotli_sources 
  CompressionPluginBrotli.cc
  BrotliCompressor.cc
)
include(ExternalProject)
ExternalProject_Add(brotli_ext
  DOWNLOAD_DIR ${CMAKE_BINARY_DIR}/src/
  GIT_REPOSITORY "https://github.com/google/brotli.git"
  GIT_TAG "master"
  SOURCE_DIR ${CMAKE_BINARY_DIR}/src/brotli
  CONFIGURE_COMMAND ./configure-cmake --disable-debug 
  INSTALL_COMMAND ""
  BUILD_COMMAND $(MAKE)
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND "")

ExternalProject_Add_Step(brotli_ext forcebuild
  DEPENDEES configure
  DEPENDERS build
  COMMAND "true"
  ALWAYS 1)

set(bortli_libs enc dec common)
foreach(lib ${bortli_libs})
  add_library(brotli::${lib} STATIC IMPORTED)
  add_dependencies(brotli::${lib} brotli_ext)
  set_property(TARGET brotli::${lib} PROPERTY IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/src/brotli/libbrotli${lib}-static.a")
  list(APPEND BROTLI_LIBRARIES brotli::${lib})
endforeach()

add_library(ceph_brotli SHARED ${brotli_sources})
add_dependencies(ceph_brotli ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_include_directories(ceph_brotli	PRIVATE "${CMAKE_BINARY_DIR}/src/brotli/c/include")
List(REVERSE bortli_libs)
target_link_libraries(ceph_brotli ${BROTLI_LIBRARIES})
install(TARGETS ceph_brotli DESTINATION ${compressor_plugin_dir})

if(WITH_EMBEDDED)
  add_library(cephd_compressor_brotli STATIC ${brotli_sources})
  target_include_directories(cephd_compressor_brotli PRIVATE "${CMAKE_BINARY_DIR}/src/brotli/c/include")
  set_target_properties(cephd_compressor_brotli PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
endif()
