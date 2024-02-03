set(librbd_types_srcs
  journal/Types.cc
  mirroring_watcher/Types.cc
  trash_watcher/Types.cc
  watcher/Types.cc
  WatchNotifyTypes.cc)

if(WITH_RBD_RWL OR WITH_RBD_SSD_CACHE)
 list(APPEND librbd_types_srcs cache/pwl/Types.cc)
endif()

add_library(rbd_types STATIC
  ${librbd_types_srcs})

if (WITH_RBD_RWL)
  target_link_libraries(rbd_types
    PUBLIC pmdk::pmemobj)
endif()

set(librbd_internal_srcs
  AsioEngine.cc
  AsyncObjectThrottle.cc
  AsyncRequest.cc
  ConfigWatcher.cc
  DeepCopyRequest.cc
  ExclusiveLock.cc
  ImageCtx.cc
  ImageState.cc
  ImageWatcher.cc
  internal.cc
  Journal.cc
  LibrbdAdminSocketHook.cc
  ManagedLock.cc
  MirroringWatcher.cc
  ObjectMap.cc
  Operations.cc
  PluginRegistry.cc
  TrashWatcher.cc
  Utils.cc
  Watcher.cc
  api/Config.cc
  api/DiffIterate.cc
  api/Group.cc
  api/Image.cc
  api/Io.cc
  api/Migration.cc
  api/Mirror.cc
  api/Namespace.cc
  api/Pool.cc
  api/PoolMetadata.cc
  api/Snapshot.cc
  api/Trash.cc
  api/Utils.cc
  asio/ContextWQ.cc
  cache/ImageWriteback.cc
  cache/ObjectCacherObjectDispatch.cc
  cache/ObjectCacherWriteback.cc
  cache/WriteAroundObjectDispatch.cc
  crypto/BlockCrypto.cc
  crypto/CryptoContextPool.cc
  crypto/CryptoObjectDispatch.cc
  crypto/FormatRequest.cc
  crypto/LoadRequest.cc
  crypto/ShutDownCryptoRequest.cc
  crypto/Utils.cc
  crypto/openssl/DataCryptor.cc
  deep_copy/ImageCopyRequest.cc
  deep_copy/MetadataCopyRequest.cc
  deep_copy/ObjectCopyRequest.cc
  deep_copy/SetHeadRequest.cc
  deep_copy/SnapshotCopyRequest.cc
  deep_copy/SnapshotCreateRequest.cc
  deep_copy/Utils.cc
  exclusive_lock/AutomaticPolicy.cc
  exclusive_lock/ImageDispatch.cc
  exclusive_lock/PreAcquireRequest.cc
  exclusive_lock/PostAcquireRequest.cc
  exclusive_lock/PreReleaseRequest.cc
  exclusive_lock/StandardPolicy.cc
  image/AttachChildRequest.cc
  image/AttachParentRequest.cc
  image/CloneRequest.cc
  image/CloseRequest.cc
  image/CreateRequest.cc
  image/DetachChildRequest.cc
  image/DetachParentRequest.cc
  image/GetMetadataRequest.cc
  image/ListWatchersRequest.cc
  image/OpenRequest.cc
  image/PreRemoveRequest.cc
  image/RefreshParentRequest.cc
  image/RefreshRequest.cc
  image/RemoveRequest.cc
  image/SetFlagsRequest.cc
  image/SetSnapRequest.cc
  image/ValidatePoolRequest.cc
  image_watcher/NotifyLockOwner.cc
  io/AioCompletion.cc
  io/AsyncOperation.cc
  io/CopyupRequest.cc
  io/FlushTracker.cc
  io/ImageDispatch.cc
  io/ImageDispatchSpec.cc
  io/ImageDispatcher.cc
  io/ImageRequest.cc
  io/ObjectDispatch.cc
  io/ObjectDispatchSpec.cc
  io/ObjectDispatcher.cc
  io/ObjectRequest.cc
  io/QosImageDispatch.cc
  io/QueueImageDispatch.cc
  io/ReadResult.cc
  io/RefreshImageDispatch.cc
  io/SimpleSchedulerObjectDispatch.cc
  io/Types.cc
  io/Utils.cc
  io/WriteBlockImageDispatch.cc
  io/IoOperations.cc
  journal/CreateRequest.cc
  journal/DemoteRequest.cc
  journal/ObjectDispatch.cc
  journal/OpenRequest.cc
  journal/PromoteRequest.cc
  journal/RemoveRequest.cc
  journal/Replay.cc
  journal/ResetRequest.cc
  journal/StandardPolicy.cc
  journal/Utils.cc
  managed_lock/AcquireRequest.cc
  managed_lock/BreakRequest.cc
  managed_lock/GetLockerRequest.cc
  managed_lock/ReacquireRequest.cc
  managed_lock/ReleaseRequest.cc
  managed_lock/Utils.cc
  migration/FileStream.cc
  migration/HttpClient.cc
  migration/HttpStream.cc
  migration/ImageDispatch.cc
  migration/NativeFormat.cc
  migration/OpenSourceImageRequest.cc
  migration/QCOWFormat.cc
  migration/RawFormat.cc
  migration/RawSnapshot.cc
  migration/S3Stream.cc
  migration/SourceSpecBuilder.cc
  migration/Utils.cc
  mirror/DemoteRequest.cc
  mirror/DisableRequest.cc
  mirror/EnableRequest.cc
  mirror/GetInfoRequest.cc
  mirror/GetStatusRequest.cc
  mirror/GetUuidRequest.cc
  mirror/ImageRemoveRequest.cc
  mirror/ImageStateUpdateRequest.cc
  mirror/PromoteRequest.cc
  mirror/snapshot/CreateNonPrimaryRequest.cc
  mirror/snapshot/CreatePrimaryRequest.cc
  mirror/snapshot/DemoteRequest.cc
  mirror/snapshot/GetImageStateRequest.cc
  mirror/snapshot/ImageMeta.cc
  mirror/snapshot/PromoteRequest.cc
  mirror/snapshot/RemoveImageStateRequest.cc
  mirror/snapshot/SetImageStateRequest.cc
  mirror/snapshot/Types.cc
  mirror/snapshot/UnlinkPeerRequest.cc
  mirror/snapshot/Utils.cc
  mirror/snapshot/WriteImageStateRequest.cc
  object_map/CreateRequest.cc
  object_map/DiffRequest.cc
  object_map/InvalidateRequest.cc
  object_map/LockRequest.cc
  object_map/RefreshRequest.cc
  object_map/RemoveRequest.cc
  object_map/Request.cc
  object_map/ResizeRequest.cc
  object_map/SnapshotCreateRequest.cc
  object_map/SnapshotRemoveRequest.cc
  object_map/SnapshotRollbackRequest.cc
  object_map/UnlockRequest.cc
  object_map/UpdateRequest.cc
  operation/DisableFeaturesRequest.cc
  operation/EnableFeaturesRequest.cc
  operation/FlattenRequest.cc
  operation/MetadataRemoveRequest.cc
  operation/MetadataSetRequest.cc
  operation/MigrateRequest.cc
  operation/ObjectMapIterate.cc
  operation/RebuildObjectMapRequest.cc
  operation/RenameRequest.cc
  operation/Request.cc
  operation/ResizeRequest.cc
  operation/SnapshotCreateRequest.cc
  operation/SnapshotProtectRequest.cc
  operation/SnapshotRemoveRequest.cc
  operation/SnapshotRenameRequest.cc
  operation/SnapshotRollbackRequest.cc
  operation/SnapshotUnprotectRequest.cc
  operation/SnapshotLimitRequest.cc
  operation/SparsifyRequest.cc
  operation/TrimRequest.cc
  plugin/Api.cc
  trash/MoveRequest.cc
  trash/RemoveRequest.cc
  watcher/Notifier.cc
  watcher/RewatchRequest.cc
  ${CMAKE_SOURCE_DIR}/src/common/ContextCompletion.cc)

if(WITH_EVENTTRACE)
  list(APPEND librbd_internal_srcs ../common/EventTrace.cc)
endif()

if(LINUX AND HAVE_LIBCRYPTSETUP)
  list(APPEND librbd_internal_srcs
          crypto/luks/LUKSEncryptionFormat.cc
          crypto/luks/FormatRequest.cc
          crypto/luks/FlattenRequest.cc
          crypto/luks/Header.cc
          crypto/luks/LoadRequest.cc
          crypto/luks/Magic.cc)
endif()

add_library(rbd_api STATIC librbd.cc)
add_library(rbd_internal STATIC
  ${librbd_internal_srcs}
  $<TARGET_OBJECTS:rados_snap_set_diff_obj>)
if(WITH_LTTNG)
  # librbd.cc includes tracing/librbd.h
  add_dependencies(rbd_api librbd-tp)
  # io/AioCompletion.cc includes tracing/librbd.h
  add_dependencies(rbd_internal librbd-tp)
endif()
if(WITH_EVENTTRACE)
  add_dependencies(rbd_internal eventtrace_tp)
endif()
target_link_libraries(rbd_internal PRIVATE
  osdc rbd_types
  OpenSSL::SSL)
target_include_directories(rbd_internal PRIVATE ${OPENSSL_INCLUDE_DIR})
if(LINUX AND HAVE_LIBCRYPTSETUP)
  target_include_directories(rbd_internal PRIVATE ${LIBCRYPTSETUP_INCLUDE_DIR})
  target_link_libraries(rbd_internal PRIVATE ${LIBCRYPTSETUP_LIBRARIES})
endif()

add_custom_target(librbd_plugins)
set(librbd_plugins_dir ${CEPH_INSTALL_PKGLIBDIR}/librbd)

set(rbd_plugin_parent_cache_srcs
  cache/ParentCacheObjectDispatch.cc
  plugin/ParentCache.cc)
add_library(librbd_plugin_parent_cache SHARED
  ${rbd_plugin_parent_cache_srcs})
target_link_libraries(librbd_plugin_parent_cache PRIVATE
  ceph_immutable_object_cache_lib ceph-common librbd
  libneorados
  librados)
set_target_properties(librbd_plugin_parent_cache PROPERTIES
  OUTPUT_NAME ceph_librbd_parent_cache
  VERSION 1.0.0
  SOVERSION 1)
install(TARGETS librbd_plugin_parent_cache DESTINATION ${librbd_plugins_dir})
add_dependencies(librbd_plugins librbd_plugin_parent_cache)

if(WITH_RBD_RWL OR WITH_RBD_SSD_CACHE)
  set(rbd_plugin_pwl_srcs
    cache/WriteLogImageDispatch.cc
    cache/pwl/AbstractWriteLog.cc
    cache/pwl/DiscardRequest.cc
    cache/pwl/ImageCacheState.cc
    cache/pwl/InitRequest.cc
    cache/pwl/LogEntry.cc
    cache/pwl/LogMap.cc
    cache/pwl/LogOperation.cc
    cache/pwl/Request.cc
    cache/pwl/ShutdownRequest.cc
    cache/pwl/SyncPoint.cc
    cache/pwl/Types.cc
    plugin/WriteLogImageCache.cc)

    if(WITH_RBD_SSD_CACHE)
      set(rbd_plugin_pwl_srcs
        ${rbd_plugin_pwl_srcs}
        cache/pwl/ssd/LogEntry.cc
        cache/pwl/ssd/LogOperation.cc
        cache/pwl/ssd/ReadRequest.cc
        cache/pwl/ssd/Request.cc
        cache/pwl/ssd/WriteLog.cc)
    endif()
    if(WITH_RBD_RWL)
      set(rbd_plugin_pwl_srcs
        ${rbd_plugin_pwl_srcs}
        cache/pwl/rwl/WriteLog.cc
        cache/pwl/rwl/LogEntry.cc
        cache/pwl/rwl/LogOperation.cc
        cache/pwl/rwl/ReadRequest.cc
        cache/pwl/rwl/Request.cc)
    endif()

  add_library(librbd_plugin_pwl_cache SHARED
    ${rbd_plugin_pwl_srcs})
  target_link_libraries(librbd_plugin_pwl_cache PRIVATE
    blk
    ceph-common
    cls_rbd_client
    libneorados
    librados
    StdFilesystem::filesystem)

  if(WITH_RBD_RWL)
    target_link_libraries(librbd_plugin_pwl_cache
      PUBLIC pmdk::pmemobj
      PRIVATE pmdk::pmem)
  endif()

  set_target_properties(librbd_plugin_pwl_cache PROPERTIES
    OUTPUT_NAME ceph_librbd_pwl_cache
    VERSION 1.0.0
    SOVERSION 1)
  install(TARGETS librbd_plugin_pwl_cache DESTINATION ${librbd_plugins_dir})
  add_dependencies(librbd_plugins librbd_plugin_pwl_cache)
endif()

add_library(librbd ${CEPH_SHARED}
  librbd.cc)
if(WITH_LTTNG)
  add_dependencies(librbd librbd-tp)
endif()

target_link_libraries(librbd PRIVATE
  rbd_internal
  rbd_types
  journal
  cls_rbd_client
  cls_lock_client
  cls_journal_client
  libneorados
  librados
  ceph-common
  pthread
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS} ${GSSAPI_LIBRARIES})
if(HAVE_UDEV)
  target_link_libraries(librbd PRIVATE
    udev)
endif()
if(ENABLE_SHARED)
  set_target_properties(librbd PROPERTIES
    OUTPUT_NAME rbd
    VERSION 1.19.0
    SOVERSION 1
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON)
    if(NOT APPLE AND NOT WIN32)
      set_property(TARGET librbd APPEND_STRING PROPERTY
        LINK_FLAGS " -Wl,--exclude-libs,ALL")
    endif()
endif(ENABLE_SHARED)
install(TARGETS librbd DESTINATION ${CMAKE_INSTALL_LIBDIR})
