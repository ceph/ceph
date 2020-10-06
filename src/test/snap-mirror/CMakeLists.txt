# driver program for snap replication
add_executable(ceph_test_snap_mirror
  ceph_test_snap_mirror.cc)
target_include_directories(ceph_test_snap_mirror SYSTEM PRIVATE "${CMAKE_SOURCE_DIR}/src/tools/cephfs_mirror")
target_link_libraries(ceph_test_snap_mirror
  cephfs_mirror_internal
  global
  ceph-common
  cls_cephfs_client
  librados
  mds
  cephfs
  ${ALLOC_LIBS})
install(TARGETS
  ceph_test_snap_mirror
  DESTINATION ${CMAKE_INSTALL_BINDIR})
