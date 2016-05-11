set(rbd_mirror_test_srcs
  test_ClusterWatcher.cc
  test_PoolWatcher.cc
  test_ImageReplayer.cc
  test_ImageSync.cc
  test_fixture.cc
  )
add_library(rbd_mirror STATIC ${rbd_mirror_test_srcs})
set_target_properties(rbd_mirror PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

add_executable(unittest_rbd_mirror EXCLUDE_FROM_ALL
  test_main.cc
  test_mock_fixture.cc
  test_mock_ImageSync.cc
  image_sync/test_mock_ImageCopyRequest.cc
  image_sync/test_mock_ObjectCopyRequest.cc
  image_sync/test_mock_SnapshotCopyRequest.cc
  image_sync/test_mock_SyncPointCreateRequest.cc
  image_sync/test_mock_SyncPointPruneRequest.cc
  ${CMAKE_SOURCE_DIR}/src/common/ContextCompletion.cc
  )
add_ceph_unittest(unittest_rbd_mirror ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_rbd_mirror)
set_target_properties(unittest_rbd_mirror PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(unittest_rbd_mirror
  rbd_mirror
  rados_test_stub
  rbd_mirror_internal
  rbd_internal
  rbd_api
  journal
  journal_test
  cls_rbd_client
  cls_lock_client
  cls_journal_client
  rbd_types
  librados
  osdc
  global
  radostest
  )

add_executable(ceph_test_rbd_mirror
  test_main.cc
  ${CMAKE_SOURCE_DIR}/src/common/ContextCompletion.cc
  )
set_target_properties(ceph_test_rbd_mirror PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rbd_mirror
  rbd_mirror
  rados_test_stub
  rbd_mirror_internal
  rbd_internal
  rbd_api
  journal
  cls_rbd_client
  cls_lock_client
  cls_journal_client
  rbd_types
  librados
  osdc
  global
  radostest
  ${UNITTEST_LIBS}
  )

