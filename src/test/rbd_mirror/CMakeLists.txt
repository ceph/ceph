set(rbd_mirror_test_srcs
  test_ClusterWatcher.cc
  test_PoolWatcher.cc
  test_ImageReplayer.cc
  test_ImageDeleter.cc
  test_ImageSync.cc
  test_fixture.cc
  )
add_library(rbd_mirror STATIC ${rbd_mirror_test_srcs})
set_target_properties(rbd_mirror PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

add_library(unittest_rbd_mirror_objs OBJECT
  test_main.cc
  test_mock_fixture.cc
  ${CMAKE_SOURCE_DIR}/src/common/ContextCompletion.cc)
set_target_properties(unittest_rbd_mirror_objs PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# some tests fully specialize a mock class, but some other test explicitly
# instantiate the same mock class using the original template. this violates
# ODR, and causes segfault. so we split the test cases into two groups
# which do not interfere with each other.
add_executable(unittest_rbd_mirror_1
  test_mock_ImageReplayer.cc
  test_mock_ImageSync.cc
  image_replayer/test_mock_BootstrapRequest.cc
  image_replayer/test_mock_CreateImageRequest.cc
  image_sync/test_mock_ObjectCopyRequest.cc
  image_sync/test_mock_SnapshotCreateRequest.cc
  $<TARGET_OBJECTS:unittest_rbd_mirror_objs>)
add_executable(unittest_rbd_mirror_2
  image_sync/test_mock_ImageCopyRequest.cc
  image_sync/test_mock_SnapshotCopyRequest.cc
  image_sync/test_mock_SyncPointCreateRequest.cc
  image_sync/test_mock_SyncPointPruneRequest.cc
  $<TARGET_OBJECTS:unittest_rbd_mirror_objs>)
foreach(unittest unittest_rbd_mirror_1 unittest_rbd_mirror_2)
  add_ceph_unittest(${unittest} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_rbd_mirror)
  set_target_properties("${unittest}" PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
  target_link_libraries(${unittest}
    rbd_mirror
    rados_test_stub
    rbd_mirror_internal
    rbd_internal
    rbd_api
    rbd_test_mock
    journal
    journal_test_mock
    cls_rbd_client
    cls_lock_client
    cls_journal_client
    rbd_types
    librados
    osdc
    global
    radostest)
endforeach()

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

add_executable(ceph_test_rbd_mirror_random_write
  random_write.cc
  $<TARGET_OBJECTS:parse_secret_objs>)
set_target_properties(ceph_test_rbd_mirror_random_write PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rbd_mirror_random_write
  librbd librados global keyutils
  )

add_executable(ceph_test_rbd_mirror_image_replay
  image_replay.cc
  ${CMAKE_SOURCE_DIR}/src/common/ContextCompletion.cc
  $<TARGET_OBJECTS:parse_secret_objs>)
set_target_properties(ceph_test_rbd_mirror_image_replay PROPERTIES
  COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rbd_mirror_image_replay
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
  librbd
  osdc
  global
  keyutils
  radostest
  ${UNITTEST_LIBS})

install(TARGETS
  ceph_test_rbd_mirror
  ceph_test_rbd_mirror_random_write
  DESTINATION ${CMAKE_INSTALL_BINDIR})
