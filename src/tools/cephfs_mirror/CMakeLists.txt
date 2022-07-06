set(cephfs_mirror_internal
  ClusterWatcher.cc
  Mirror.cc
  FSMirror.cc
  InstanceWatcher.cc
  MirrorWatcher.cc
  PeerReplayer.cc
  ServiceDaemon.cc
  Types.cc
  Utils.cc
  Watcher.cc
  watcher/RewatchRequest.cc)

add_executable(cephfs-mirror
  main.cc)

add_library(cephfs_mirror_internal STATIC
  ${cephfs_mirror_internal})

target_link_libraries(cephfs-mirror
  cephfs_mirror_internal
  global
  ceph-common
  cls_cephfs_client
  librados
  mds
  cephfs
  ${ALLOC_LIBS})

install(TARGETS cephfs-mirror DESTINATION bin)
