set(ceph_rwl_replica_files
  EventOp.cc
  MemoryManager.cc
  Reactor.cc
  Types.cc
  ReplicaClient.cc
  TimerPing.cc
  )

add_library(ceph_rwl_replica_lib STATIC ${ceph_rwl_replica_files})

target_link_libraries(ceph_rwl_replica_lib
  pmdk::pmem
  rpma::rpma
  librados
  librbd
  global
  ceph-common
  cls_rbd_client
  libneorados
  )

add_executable(ceph-rwl-replica main.cc)
target_link_libraries(ceph-rwl-replica
  ceph_rwl_replica_lib
  )
install(TARGETS ceph-rwl-replica DESTINATION bin)
