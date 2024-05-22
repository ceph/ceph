add_library(compressor_objs OBJECT Compressor.cc)
add_dependencies(compressor_objs common-objs)
target_link_libraries(compressor_objs legacy-option-headers)

if(HAVE_QATZIP AND HAVE_QAT)
  add_library(qat_compressor OBJECT QatAccel.cc)
  target_link_libraries(qat_compressor PUBLIC
                        QAT::qat
                        QAT::usdm
                        QAT::zip
                       )
endif()

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

add_library(compressor STATIC)
target_link_libraries(compressor PUBLIC compressor_objs)

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
