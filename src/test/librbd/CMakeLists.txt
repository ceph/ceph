set(librbd_test
  test_fixture.cc
  test_support.cc
  test_librbd.cc
  test_ImageWatcher.cc
  test_internal.cc
  test_mirroring.cc
  test_BlockGuard.cc
  test_DeepCopy.cc
  test_Groups.cc
  test_MirroringWatcher.cc
  test_ObjectMap.cc
  test_Operations.cc
  journal/test_Entries.cc
  journal/test_Replay.cc)
add_library(rbd_test STATIC ${librbd_test})
set_target_properties(rbd_test PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

set(librbd_test_mock_srcs
  mock/MockImageCtx.cc
  mock/MockJournal.cc)
add_library(rbd_test_mock STATIC ${librbd_test_mock_srcs})
set_target_properties(rbd_test_mock PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_librbd
# doesn't use add_ceph_test because it is called by run-rbd-unit-tests.sh
set(unittest_librbd_srcs
  test_main.cc
  test_mock_fixture.cc
  test_mock_DeepCopyRequest.cc
  test_mock_ExclusiveLock.cc
  test_mock_Journal.cc
  test_mock_ManagedLock.cc
  test_mock_ObjectMap.cc
  test_mock_TrashWatcher.cc
  test_mock_Watcher.cc
  deep_copy/test_mock_ImageCopyRequest.cc
  deep_copy/test_mock_MetadataCopyRequest.cc
  deep_copy/test_mock_ObjectCopyRequest.cc
  deep_copy/test_mock_SetHeadRequest.cc
  deep_copy/test_mock_SnapshotCopyRequest.cc
  deep_copy/test_mock_SnapshotCreateRequest.cc
  exclusive_lock/test_mock_PreAcquireRequest.cc
  exclusive_lock/test_mock_PostAcquireRequest.cc
  exclusive_lock/test_mock_PreReleaseRequest.cc
  image/test_mock_CloneRequest.cc
  image/test_mock_DetachChildRequest.cc
  image/test_mock_RefreshRequest.cc
  image/test_mock_RemoveRequest.cc
  image/test_mock_ValidatePoolRequest.cc
  io/test_mock_ImageRequest.cc
  io/test_mock_ImageRequestWQ.cc
  io/test_mock_ObjectRequest.cc
  journal/test_mock_OpenRequest.cc
  journal/test_mock_PromoteRequest.cc
  journal/test_mock_Replay.cc
  journal/test_mock_ResetRequest.cc
  managed_lock/test_mock_AcquireRequest.cc
  managed_lock/test_mock_BreakRequest.cc
  managed_lock/test_mock_GetLockerRequest.cc
  managed_lock/test_mock_ReacquireRequest.cc
  managed_lock/test_mock_ReleaseRequest.cc
  mirror/test_mock_DisableRequest.cc
  object_map/test_mock_InvalidateRequest.cc
  object_map/test_mock_LockRequest.cc
  object_map/test_mock_RefreshRequest.cc
  object_map/test_mock_ResizeRequest.cc
  object_map/test_mock_SnapshotCreateRequest.cc
  object_map/test_mock_SnapshotRemoveRequest.cc
  object_map/test_mock_SnapshotRollbackRequest.cc
  object_map/test_mock_UnlockRequest.cc
  object_map/test_mock_UpdateRequest.cc
  operation/test_mock_DisableFeaturesRequest.cc
  operation/test_mock_EnableFeaturesRequest.cc
  operation/test_mock_Request.cc
  operation/test_mock_ResizeRequest.cc
  operation/test_mock_SnapshotCreateRequest.cc
  operation/test_mock_SnapshotProtectRequest.cc
  operation/test_mock_SnapshotRemoveRequest.cc
  operation/test_mock_SnapshotRollbackRequest.cc
  operation/test_mock_SnapshotUnprotectRequest.cc
  operation/test_mock_TrimRequest.cc
  trash/test_mock_MoveRequest.cc
  trash/test_mock_RemoveRequest.cc
  watcher/test_mock_RewatchRequest.cc
  )
add_executable(unittest_librbd
  ${unittest_librbd_srcs}
  $<TARGET_OBJECTS:common_texttable_obj>)
target_compile_definitions(unittest_librbd PUBLIC "-DTEST_LIBRBD_INTERNALS")
set_target_properties(unittest_librbd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(unittest_librbd
  cls_rbd
  cls_rbd_client
  cls_lock
  cls_lock_client
  journal
  journal_test_mock
  cls_journal
  cls_journal_client
  rados_test_stub
  librados
  rbd_test
  rbd_test_mock
  rbd_api
  rbd_internal
  rbd_types
  osdc
  global
  ${UNITTEST_LIBS}
  radostest)

add_executable(ceph_test_librbd
  test_main.cc
  $<TARGET_OBJECTS:common_texttable_obj>)
target_link_libraries(ceph_test_librbd
  rbd_test
  rbd_api
  rbd_internal
  rbd_types
  journal
  cls_journal_client
  cls_rbd_client
  librados
  ${UNITTEST_LIBS}
  radostest)
target_compile_definitions(ceph_test_librbd PUBLIC "-DTEST_LIBRBD_INTERNALS")
set_target_properties(ceph_test_librbd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

add_executable(ceph_test_librbd_api
  test_support.cc
  test_librbd.cc
  test_main.cc
  $<TARGET_OBJECTS:libradostest_obj>
  $<TARGET_OBJECTS:common_texttable_obj>)
target_link_libraries(ceph_test_librbd_api
  librbd
  librados
  ${UNITTEST_LIBS})
set_target_properties(ceph_test_librbd_api PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

add_executable(ceph_test_librbd_fsx
  fsx.cc
  $<TARGET_OBJECTS:common_texttable_obj>
  )
target_link_libraries(ceph_test_librbd_fsx
  librbd
  librados
  journal
  global
  m
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  )
if(WITH_KRBD)
  target_link_libraries(ceph_test_librbd_fsx
    krbd)
endif()
install(TARGETS
  ceph_test_librbd_fsx
  DESTINATION ${CMAKE_INSTALL_BINDIR})

install(TARGETS
  ceph_test_librbd
  ceph_test_librbd_api
  DESTINATION ${CMAKE_INSTALL_BINDIR})
