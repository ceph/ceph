include(MergeStaticLibraries)

add_library(cephd_base STATIC
  libcephd.cc
  ../ceph_mon.cc
  ../ceph_osd.cc
  ../ceph_mds.cc)

set_target_properties(cephd_base PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)

set(merge_libs
  cephd_base
  cephd_compressor
  cephd_ec
  cephd_cls
  cephd_cls_kvs
  cephd_rados
  common
  common_utf8
  erasure_code
  global
  json_spirit
  kv
  mds
  mon
  os
  osd
  osdc)

if(NOT WITH_SYSTEM_ROCKSDB)
  list(APPEND merge_libs ${ROCKSDB_LIBRARIES})
endif(NOT WITH_SYSTEM_ROCKSDB)

if(WITH_RADOSGW)
  list(APPEND merge_libs cephd_rgw)
endif(WITH_RADOSGW)

if(WITH_RBD)
  list(APPEND merge_libs cephd_rbd)
endif(WITH_RBD)

if(HAVE_ARMV8_CRC)
  list(APPEND merge_libs common_crc_aarch64)
endif(HAVE_ARMV8_CRC)

merge_static_libraries(cephd ${merge_libs})

# TODO: install these libraries and add them to rpm and deb packages
#install(TARGETS cephd DESTINATION ${CMAKE_INSTALL_LIBDIR})
#install(FILES ../include/cephd/libcephd.h
#  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/cephd)
