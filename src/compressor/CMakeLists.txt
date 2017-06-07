
set(compressor_srcs 
  Compressor.cc
  AsyncCompressor.cc)
add_library(compressor_objs OBJECT ${compressor_srcs})

## compressor plugins

set(compressor_plugin_dir ${CMAKE_INSTALL_PKGLIBDIR}/compressor)

add_subdirectory(snappy)
add_subdirectory(zlib)
add_subdirectory(zstd)

add_custom_target(compressor_plugins DEPENDS
    ceph_snappy
    ceph_zlib
    ceph_zstd)

if(WITH_EMBEDDED)
  include(MergeStaticLibraries)
  add_library(cephd_compressor_base STATIC ${compressor_srcs})
  set_target_properties(cephd_compressor_base PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
  merge_static_libraries(cephd_compressor cephd_compressor_base cephd_compressor_snappy cephd_compressor_zlib cephd_compressor_zstd)
endif()
