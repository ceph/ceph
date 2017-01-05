add_library(rbd_types STATIC
  journal/Types.cc
  mirroring_watcher/Types.cc
  WatchNotifyTypes.cc)

set(librbd_internal_srcs
  AioCompletion.cc
  AioImageRequest.cc
  AioImageRequestWQ.cc
  AioObjectRequest.cc
  AsyncObjectThrottle.cc
  AsyncOperation.cc
  AsyncRequest.cc
  CopyupRequest.cc
  DiffIterate.cc
  ExclusiveLock.cc
  Group.cc
  ImageCtx.cc
  ImageState.cc
  ImageWatcher.cc
  internal.cc
  Journal.cc
  LibrbdAdminSocketHook.cc
  LibrbdWriteback.cc
  MirroringWatcher.cc
  ObjectMap.cc
  ObjectWatcher.cc
  Operations.cc
  Utils.cc
  cache/ImageWriteback.cc
  cache/PassthroughImageCache.cc
  exclusive_lock/AcquireRequest.cc
  exclusive_lock/AutomaticPolicy.cc
  exclusive_lock/ReacquireRequest.cc
  exclusive_lock/ReleaseRequest.cc
  exclusive_lock/StandardPolicy.cc
  image/CloseRequest.cc
  image/CreateRequest.cc
  image/OpenRequest.cc
  image/RefreshParentRequest.cc
  image/RefreshRequest.cc
  image/SetFlagsRequest.cc
  image/SetSnapRequest.cc
  image_watcher/NotifyLockOwner.cc
  image_watcher/RewatchRequest.cc
  journal/RemoveRequest.cc
  journal/CreateRequest.cc
  journal/OpenRequest.cc
  journal/PromoteRequest.cc
  journal/Replay.cc
  journal/StandardPolicy.cc
  journal/Utils.cc
  mirror/DisableRequest.cc
  mirror/EnableRequest.cc
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
  object_watcher/Notifier.cc
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
  common
  pthread
  udev
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS})
if(ENABLE_SHARED)
  set_target_properties(librbd PROPERTIES
    OUTPUT_NAME rbd
    VERSION 1.0.0
    SOVERSION 1
    COMPILE_FLAGS "-fvisibility=hidden -fvisibility-inlines-hidden"
    LINK_FLAGS "-Wl,--exclude-libs,ALL")
endif(ENABLE_SHARED)
install(TARGETS librbd DESTINATION ${CMAKE_INSTALL_LIBDIR})

if(WITH_EMBEDDED)
  add_library(cephd_rbd_base STATIC librbd.cc ${CMAKE_SOURCE_DIR}/src/common/ContextCompletion.cc)
  merge_static_libraries(cephd_rbd cephd_rbd_base rbd_internal rbd_types journal)
endif()
