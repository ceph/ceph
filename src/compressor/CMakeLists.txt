
set(compressor_srcs 
  Compressor.cc)
add_library(compressor_objs OBJECT ${compressor_srcs})

## compressor plugins

set(compressor_plugin_dir ${CMAKE_INSTALL_PKGLIBDIR}/compressor)

add_subdirectory(snappy)
add_subdirectory(zlib)
add_subdirectory(zstd)

if(HAVE_LZ4)
  add_subdirectory(lz4)
endif()

if(HAVE_BROTLI)
  add_subdirectory(brotli)
endif()

set(ceph_compressor_libs
    ceph_snappy
    ceph_zlib
    ceph_zstd)

if(HAVE_LZ4)
  list(APPEND ceph_compressor_libs ceph_lz4)
endif()

if(HAVE_BROTLI)
  list(APPEND ceph_compressor_libs ceph_brotli)
endif()

add_custom_target(compressor_plugins DEPENDS
    ${ceph_compressor_libs})

if(WITH_EMBEDDED)
  include(MergeStaticLibraries)
  add_library(cephd_compressor_base STATIC ${compressor_srcs})
  set_target_properties(cephd_compressor_base PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
  set(cephd_compressor_libs
      cephd_compressor_base
      cephd_compressor_snappy
      cephd_compressor_zlib
      cephd_compressor_zstd)
  if(HAVE_LZ4)
    list(APPEND cephd_compressor_libs cephd_compressor_lz4)
  endif()
  if(HAVE_BROTLI)
    list(APPEND cephd_compressor_libs cephd_compressor_brotli)
  endif()
  merge_static_libraries(cephd_compressor ${cephd_compressor_libs})
endif()
