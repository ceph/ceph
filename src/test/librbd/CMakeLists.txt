set(librbd_test_support_srcs
  test_support.cc
  )
add_library(rbd_test_support STATIC ${librbd_test_support_srcs})
target_link_libraries(rbd_test_support PRIVATE
  GTest::GTest)

set(librbd_test
  test_fixture.cc
  test_librbd.cc
  test_ImageWatcher.cc
  test_internal.cc
  test_mirroring.cc
  test_DeepCopy.cc
  test_Groups.cc
  test_Migration.cc
  test_MirroringWatcher.cc
  test_ObjectMap.cc
  test_Operations.cc
  test_Trash.cc
  journal/test_Entries.cc
  journal/test_Replay.cc
  journal/test_Stress.cc)
add_library(rbd_test STATIC ${librbd_test})
target_link_libraries(rbd_test PRIVATE
  rbd_test_support
  radostest
  radostest-cxx
  librados
  Boost::thread
  GMock::GMock
  GTest::GTest)

set(librbd_test_mock_srcs
  mock/MockImageCtx.cc
  mock/MockJournal.cc)
add_library(rbd_test_mock STATIC ${librbd_test_mock_srcs})
target_link_libraries(rbd_test_mock PUBLIC
  GMock::GMock)

# unittest_librbd
# doesn't use add_ceph_test because it is called by run-rbd-unit-tests.sh
set(unittest_librbd_srcs
  test_BlockGuard.cc
  test_main.cc
  test_mock_fixture.cc
  test_mock_ConfigWatcher.cc
  test_mock_DeepCopyRequest.cc
  test_mock_ExclusiveLock.cc
  test_mock_Journal.cc
  test_mock_ManagedLock.cc
  test_mock_ObjectMap.cc
  test_mock_TrashWatcher.cc
  test_mock_Watcher.cc
  cache/test_mock_WriteAroundObjectDispatch.cc
  cache/test_mock_ParentCacheObjectDispatch.cc
  crypto/test_mock_BlockCrypto.cc
  crypto/test_mock_CryptoContextPool.cc
  crypto/test_mock_CryptoObjectDispatch.cc
  crypto/test_mock_FormatRequest.cc
  crypto/test_mock_LoadRequest.cc
  crypto/test_mock_ShutDownCryptoRequest.cc
  crypto/openssl/test_DataCryptor.cc
  deep_copy/test_mock_ImageCopyRequest.cc
  deep_copy/test_mock_MetadataCopyRequest.cc
  deep_copy/test_mock_ObjectCopyRequest.cc
  deep_copy/test_mock_SetHeadRequest.cc
  deep_copy/test_mock_SnapshotCopyRequest.cc
  deep_copy/test_mock_SnapshotCreateRequest.cc
  exclusive_lock/test_mock_PreAcquireRequest.cc
  exclusive_lock/test_mock_PostAcquireRequest.cc
  exclusive_lock/test_mock_PreReleaseRequest.cc
  image/test_mock_AttachChildRequest.cc
  image/test_mock_AttachParentRequest.cc
  image/test_mock_CloneRequest.cc
  image/test_mock_DetachChildRequest.cc
  image/test_mock_DetachParentRequest.cc
  image/test_mock_ListWatchersRequest.cc
  image/test_mock_PreRemoveRequest.cc
  image/test_mock_RefreshRequest.cc
  image/test_mock_RemoveRequest.cc
  image/test_mock_ValidatePoolRequest.cc
  io/test_mock_CopyupRequest.cc
  io/test_mock_ImageRequest.cc
  io/test_mock_ObjectRequest.cc
  io/test_mock_SimpleSchedulerObjectDispatch.cc
  journal/test_mock_OpenRequest.cc
  journal/test_mock_PromoteRequest.cc
  journal/test_mock_Replay.cc
  journal/test_mock_ResetRequest.cc
  managed_lock/test_mock_AcquireRequest.cc
  managed_lock/test_mock_BreakRequest.cc
  managed_lock/test_mock_GetLockerRequest.cc
  managed_lock/test_mock_ReacquireRequest.cc
  managed_lock/test_mock_ReleaseRequest.cc
  migration/test_mock_FileStream.cc
  migration/test_mock_HttpClient.cc
  migration/test_mock_HttpStream.cc
  migration/test_mock_RawFormat.cc
  migration/test_mock_RawSnapshot.cc
  migration/test_mock_QCOWFormat.cc
  migration/test_mock_S3Stream.cc
  migration/test_mock_Utils.cc
  mirror/snapshot/test_mock_CreateNonPrimaryRequest.cc
  mirror/snapshot/test_mock_CreatePrimaryRequest.cc
  mirror/snapshot/test_mock_ImageMeta.cc
  mirror/snapshot/test_mock_PromoteRequest.cc
  mirror/snapshot/test_mock_UnlinkPeerRequest.cc
  mirror/snapshot/test_mock_Utils.cc
  mirror/test_mock_DisableRequest.cc
  object_map/test_mock_DiffRequest.cc
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

if(WITH_RBD_RWL OR WITH_RBD_SSD_CACHE)
   list(APPEND unittest_librbd_srcs
     cache/pwl/test_WriteLogMap.cc)
   if(WITH_RBD_RWL)
     list(APPEND unittest_librbd_srcs
       cache/pwl/test_mock_ReplicatedWriteLog.cc)
   endif()
   if(WITH_RBD_SSD_CACHE)
     list(APPEND unittest_librbd_srcs
       cache/pwl/test_mock_SSDWriteLog.cc)
   endif()
endif()

if(LINUX AND HAVE_LIBCRYPTSETUP)
  list(APPEND unittest_librbd_srcs
          crypto/luks/test_mock_FlattenRequest.cc
          crypto/luks/test_mock_FormatRequest.cc
          crypto/luks/test_mock_LoadRequest.cc)
endif()

# On Windows, we'll skip librbd unit tests for the time being, running just the
# functional tests. The reason is that the unit tests require libcls*, which in
# turn requires libos and libosd, however those libraries haven't been ported to
# Windows.
if(NOT WIN32)
add_executable(unittest_librbd
  ${unittest_librbd_srcs}
  $<TARGET_OBJECTS:common_texttable_obj>)
target_compile_definitions(unittest_librbd PRIVATE "TEST_LIBRBD_INTERNALS")
set_target_properties(unittest_librbd PROPERTIES
  JOB_POOL_COMPILE heavy_compile_job_pool
  JOB_POOL_LINK heavy_link_job_pool)
add_dependencies(unittest_librbd
  cls_journal
  cls_lock
  cls_rbd)
target_link_libraries(unittest_librbd
  rbd_test
  rbd_api
  rbd_internal
  rbd_test_mock
  journal
  journal_test_mock
  cls_rbd_client
  cls_lock_client
  cls_journal_client
  rbd_types
  rados_test_stub
  librados
  ceph_immutable_object_cache_lib
  osdc
  ceph-common
  global
  OpenSSL::SSL
  ${UNITTEST_LIBS})

if(WITH_RBD_RWL OR WITH_RBD_SSD_CACHE)
  target_link_libraries(unittest_librbd
    librbd_plugin_pwl_cache)
endif()
endif(NOT WIN32)

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
  libneorados
  librados
  ${UNITTEST_LIBS}
  radostest)
target_compile_definitions(ceph_test_librbd PRIVATE "TEST_LIBRBD_INTERNALS")

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
  DESTINATION ${CMAKE_INSTALL_BINDIR})
