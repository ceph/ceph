include(MergeStaticLibraries)

add_library(cephd_base STATIC
  libcephd.cc)

set(merge_libs
  cephd_base
  mon
  osd
  common
  common_utf8
  os
  kv
  global
  rocksdb
  json_spirit
  erasure_code
  cephd_compressor
  cephd_ec)

if(HAVE_ARMV8_CRC)
  list(APPEND merge_libs common_crc_aarch64)
endif(HAVE_ARMV8_CRC)

merge_static_libraries(cephd ${merge_libs})

# TODO: install these libraries and add them to rpm and deb packages
#install(TARGETS cephd DESTINATION ${CMAKE_INSTALL_LIBDIR})
#install(FILES ../include/cephd/libcephd.h DESTINATION include/cephd)
