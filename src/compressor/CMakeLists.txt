## compressor plugins

set(compressorlibdir ${LIBRARY_OUTPUT_PATH}/compressor)

add_subdirectory(snappy)

add_library(compressor_objs OBJECT Compressor.cc)

add_custom_target(compressor_plugins DEPENDS
    ceph_snappy)
