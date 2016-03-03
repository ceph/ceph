
set(compressor_srcs 
  Compressor.cc
  AsyncCompressor.cc)
add_library(compressor STATIC ${compressor_srcs})
target_link_libraries(compressor common snappy ${CMAKE_DL_LIBS})

## compressor plugins

set(compressorlibdir ${LIBRARY_OUTPUT_PATH}/compressor)

add_subdirectory(snappy)
add_subdirectory(zlib)

add_library(compressor_objs OBJECT Compressor.cc)

add_custom_target(compressor_plugins DEPENDS
    ceph_snappy
    ceph_zlib)
