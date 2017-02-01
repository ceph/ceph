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
  exclusive_lock/AcquireRequest.cc
  exclusive_lock/AutomaticPolicy.cc
  exclusive_lock/BreakRequest.cc
  exclusive_lock/GetLockerRequest.cc
  exclusive_lock/ReacquireRequest.cc
  exclusive_lock/ReleaseRequest.cc
  exclusive_lock/StandardPolicy.cc
  image/CloseRequest.cc
  image/OpenRequest.cc
  image/RefreshParentRequest.cc
  image/RefreshRequest.cc
  image/SetSnapRequest.cc
  image_watcher/Notifier.cc
  image_watcher/NotifyLockOwner.cc
  image_watcher/RewatchRequest.cc
  journal/Replay.cc
  journal/StandardPolicy.cc
  object_map/InvalidateRequest.cc
  object_map/LockRequest.cc
  object_map/Request.cc
  object_map/RefreshRequest.cc
  object_map/ResizeRequest.cc
  object_map/SnapshotCreateRequest.cc
  object_map/SnapshotRemoveRequest.cc
  object_map/SnapshotRollbackRequest.cc
  object_map/UnlockRequest.cc
  object_map/UpdateRequest.cc
  operation/FlattenRequest.cc
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
  operation/TrimRequest.cc)

add_library(rbd_api STATIC librbd.cc)
add_library(rbd_internal STATIC ${librbd_internal_srcs})

add_library(librbd ${CEPH_SHARED}
  $<TARGET_OBJECTS:osdc_rbd_objs>
  $<TARGET_OBJECTS:common_util_obj>
  $<TARGET_OBJECTS:krbd_objs>
  ${CMAKE_SOURCE_DIR}/src/common/ContextCompletion.cc
  librbd.cc)
target_link_libraries(librbd PRIVATE 
  rbd_internal
  rbd_types
  journal
  librados 
  common 
  osdc
  cls_rbd_client 
  cls_lock_client 
  cls_journal_client 
  pthread
  udev
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS})
if(${ENABLE_SHARED})
  set_target_properties(librbd PROPERTIES VERSION "1.0.0" SOVERSION "1"
    OUTPUT_NAME rbd)
endif(${ENABLE_SHARED})
install(TARGETS librbd DESTINATION lib)
