add_library(rbd_types STATIC
  journal/Types.cc
  mirroring_watcher/Types.cc
  trash_watcher/Types.cc
  watcher/Types.cc
  WatchNotifyTypes.cc)

set(librbd_internal_srcs
  AsioEngine.cc
  AsyncObjectThrottle.cc
  AsyncRequest.cc
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
  asio/ContextWQ.cc
  cache/ImageWriteback.cc
  cache/ObjectCacherObjectDispatch.cc
  cache/ObjectCacherWriteback.cc
  cache/PassthroughImageCache.cc
  cache/WriteAroundObjectDispatch.cc
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
  io/Utils.cc
  io/WriteBlockImageDispatch.cc
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
  trash/MoveRequest.cc
  trash/RemoveRequest.cc
  watcher/Notifier.cc
  watcher/RewatchRequest.cc
  ${CMAKE_SOURCE_DIR}/src/common/ContextCompletion.cc)

add_custom_target(librbd_plugins)
set(librbd_plugins_dir ${CEPH_INSTALL_PKGLIBDIR}/librbd)

set(rbd_plugin_parent_cache_srcs
  cache/ParentCacheObjectDispatch.cc
  plugin/ParentCache.cc)
add_library(librbd_plugin_parent_cache SHARED ${rbd_plugin_parent_cache_srcs})
target_link_libraries(librbd_plugin_parent_cache PRIVATE
  ceph_immutable_object_cache_lib)
set_target_properties(librbd_plugin_parent_cache PROPERTIES
  OUTPUT_NAME ceph_librbd_parent_cache
  VERSION 1.0.0
  SOVERSION 1)
install(TARGETS librbd_plugin_parent_cache DESTINATION ${librbd_plugins_dir})
add_dependencies(librbd_plugins librbd_plugin_parent_cache)

if(WITH_EVENTTRACE)
  list(APPEND librbd_internal_srcs ../common/EventTrace.cc)
endif()

if(WITH_RBD_RWL)
  set(librbd_internal_srcs
    ${librbd_internal_srcs}
    cache/rwl/ImageCacheState.cc
    cache/rwl/LogEntry.cc
    cache/rwl/LogMap.cc
    cache/rwl/LogOperation.cc
    cache/rwl/Request.cc
    cache/rwl/SyncPoint.cc
    cache/rwl/Types.cc
    cache/ReplicatedWriteLog.cc)
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
  osdc)

if(WITH_RBD_RWL)
  target_link_libraries(rbd_internal
    PUBLIC blk)
endif()

add_library(librbd ${CEPH_SHARED}
  librbd.cc)
if(WITH_LTTNG)
  add_dependencies(librbd librbd-tp)
endif()
add_dependencies(librbd librbd_plugins)

target_link_libraries(librbd PRIVATE
  rbd_internal
  rbd_types
  journal
  libneorados
  librados
  cls_rbd_client
  cls_lock_client
  cls_journal_client
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
    VERSION 1.12.0
    SOVERSION 1
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON)
    if(NOT APPLE)
      set_property(TARGET librbd APPEND_STRING PROPERTY
        LINK_FLAGS " -Wl,--exclude-libs,ALL")
    endif()
endif(ENABLE_SHARED)
install(TARGETS librbd DESTINATION ${CMAKE_INSTALL_LIBDIR})
