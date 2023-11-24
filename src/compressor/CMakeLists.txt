
set(compressor_srcs
  Compressor.cc)
if (HAVE_QATZIP)
  list(APPEND compressor_srcs QatAccel.cc)
endif()
add_library(compressor_objs OBJECT ${compressor_srcs})
add_dependencies(compressor_objs common-objs)
if(HAVE_QATZIP AND HAVE_QAT)
  target_link_libraries(compressor_objs PRIVATE
                        QAT::qat
                        QAT::usdm
                        qatzip::qatzip
                       )
endif()
add_dependencies(compressor_objs legacy-option-headers)

## compressor plugins

set(compressor_plugin_dir ${CEPH_INSTALL_PKGLIBDIR}/compressor)

add_subdirectory(snappy)
add_subdirectory(zlib)
add_subdirectory(zstd)

if(HAVE_LZ4)
  add_subdirectory(lz4)
endif()

if(HAVE_BROTLI)
  add_subdirectory(brotli)
endif()

add_library(compressor STATIC $<TARGET_OBJECTS:compressor_objs>)
target_link_libraries(compressor PRIVATE compressor_objs)

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
