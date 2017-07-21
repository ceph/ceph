add_library(rbd_mirror_types STATIC
  instance_watcher/Types.cc
  leader_watcher/Types.cc)

set(rbd_mirror_internal
  ClusterWatcher.cc
  ImageDeleter.cc
  ImageReplayer.cc
  ImageSync.cc
  ImageSyncThrottler.cc
  InstanceReplayer.cc
  InstanceWatcher.cc
  Instances.cc
  LeaderWatcher.cc
  Mirror.cc
  MirrorStatusWatcher.cc
  PoolReplayer.cc
  PoolWatcher.cc
  Threads.cc
  types.cc
  image_replayer/BootstrapRequest.cc
  image_replayer/CloseImageRequest.cc
  image_replayer/CreateImageRequest.cc
  image_replayer/EventPreprocessor.cc
  image_replayer/IsPrimaryRequest.cc
  image_replayer/OpenImageRequest.cc
  image_replayer/OpenLocalImageRequest.cc
  image_replayer/PrepareLocalImageRequest.cc
  image_replayer/ReplayStatusFormatter.cc
  image_sync/ImageCopyRequest.cc
  image_sync/ObjectCopyRequest.cc
  image_sync/SnapshotCopyRequest.cc
  image_sync/SnapshotCreateRequest.cc
  image_sync/SyncPointCreateRequest.cc
  image_sync/SyncPointPruneRequest.cc
  pool_watcher/RefreshImagesRequest.cc)
add_library(rbd_mirror_internal STATIC
  ${rbd_mirror_internal})

add_executable(rbd-mirror
  main.cc)
target_link_libraries(rbd-mirror
  rbd_mirror_internal
  rbd_mirror_types
  rbd_api
  rbd_internal
  rbd_types
  journal
  librados
  osdc
  cls_rbd_client
  cls_lock_client
  cls_journal_client
  global)
install(TARGETS rbd-mirror DESTINATION bin)
