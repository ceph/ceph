add_library(rbd_types STATIC
  journal/Types.cc
  mirroring_watcher/Types.cc
  watcher/Types.cc
  WatchNotifyTypes.cc)

set(librbd_internal_srcs
  AsyncObjectThrottle.cc
  AsyncRequest.cc
  ExclusiveLock.cc
  ImageCtx.cc
  ImageState.cc
  ImageWatcher.cc
  internal.cc
  Journal.cc
  LibrbdAdminSocketHook.cc
  LibrbdWriteback.cc
  ManagedLock.cc
  MirroringWatcher.cc
  ObjectMap.cc
  Operations.cc
  Utils.cc
  Watcher.cc
  api/DiffIterate.cc
  api/Group.cc
  api/Image.cc
  api/Mirror.cc
  cache/ImageWriteback.cc
  cache/PassthroughImageCache.cc
  exclusive_lock/AutomaticPolicy.cc
  exclusive_lock/PreAcquireRequest.cc
  exclusive_lock/PostAcquireRequest.cc
  exclusive_lock/PreReleaseRequest.cc
  exclusive_lock/StandardPolicy.cc
  image/CloneRequest.cc
  image/CloseRequest.cc
  image/CreateRequest.cc
  image/OpenRequest.cc
  image/RefreshParentRequest.cc
  image/RefreshRequest.cc
  image/RemoveRequest.cc
  image/SetFlagsRequest.cc
  image/SetSnapRequest.cc
  image_watcher/NotifyLockOwner.cc
  io/AioCompletion.cc
  io/AsyncOperation.cc
  io/CopyupRequest.cc
  io/ImageRequest.cc
  io/ImageRequestWQ.cc
  io/ObjectRequest.cc
  io/ReadResult.cc
  journal/CreateRequest.cc
  journal/DemoteRequest.cc
  journal/OpenRequest.cc
  journal/PromoteRequest.cc
  journal/RemoveRequest.cc
  journal/Replay.cc
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
  mirror/PromoteRequest.cc
  object_map/CreateRequest.cc
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
  operation/TrimRequest.cc
  watcher/Notifier.cc
  watcher/RewatchRequest.cc
  ${CMAKE_SOURCE_DIR}/src/common/ContextCompletion.cc)

add_library(rbd_api STATIC librbd.cc)
add_library(rbd_internal STATIC
  ${librbd_internal_srcs}
  $<TARGET_OBJECTS:rados_snap_set_diff_obj>)
if(WITH_LTTNG)
  add_dependencies(rbd_internal librbd-tp)
endif()

add_library(librbd ${CEPH_SHARED}
  librbd.cc)

target_link_libraries(librbd LINK_PRIVATE 
  rbd_internal
  rbd_types
  journal
  librados 
  osdc
  cls_rbd_client 
  cls_lock_client 
  cls_journal_client 
  ceph-common
  pthread
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS})
if(HAVE_UDEV)
  target_link_libraries(librbd LINK_PRIVATE
    udev)
endif()
if(ENABLE_SHARED)
  set_target_properties(librbd PROPERTIES
    OUTPUT_NAME rbd
    VERSION 1.12.0
    SOVERSION 1
    COMPILE_FLAGS "-fvisibility=hidden -fvisibility-inlines-hidden"
    LINK_FLAGS "-Wl,--exclude-libs,ALL")
endif(ENABLE_SHARED)
install(TARGETS librbd DESTINATION ${CMAKE_INSTALL_LIBDIR})

if(WITH_EMBEDDED)
  add_library(cephd_rbd_base STATIC librbd.cc ${CMAKE_SOURCE_DIR}/src/common/ContextCompletion.cc)
  merge_static_libraries(cephd_rbd cephd_rbd_base rbd_internal rbd_types journal)
endif()
