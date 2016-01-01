## Rados object classes

# cls_hello
add_library(cls_hello SHARED hello/cls_hello.cc)
set_target_properties(cls_hello PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS cls_hello DESTINATION lib/rados-classes)

# cls_rbd
if (WITH_RBD)
  add_library(cls_rbd SHARED rbd/cls_rbd.cc rbd/cls_rbd_types.cc)
  set_target_properties(cls_rbd PROPERTIES VERSION "1.0.0" SOVERSION "1")
  install(TARGETS cls_rbd DESTINATION lib/rados-classes)

  add_library(cls_rbd_client rbd/cls_rbd_client.cc rbd/cls_rbd_types.cc)
endif (WITH_RBD)

# cls_lock
add_library(cls_lock SHARED lock/cls_lock.cc)
set_target_properties(cls_lock PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS cls_lock DESTINATION lib/rados-classes)

add_library(cls_lock_client
  lock/cls_lock_client.cc
  lock/cls_lock_types.cc
  lock/cls_lock_ops.cc)

# cls_refcount
add_library(cls_refcount SHARED
  refcount/cls_refcount.cc
  refcount/cls_refcount_ops.cc
  ${CMAKE_SOURCE_DIR}/src/common/ceph_json.cc)
target_link_libraries(cls_refcount json_spirit)
set_target_properties(cls_refcount PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS cls_refcount DESTINATION lib/rados-classes)

add_library(cls_refcount_client
  refcount/cls_refcount_client.cc
  refcount/cls_refcount_ops.cc)

# cls_version
add_library(cls_version SHARED version/cls_version.cc)
set_target_properties(cls_version PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS cls_version DESTINATION lib/rados-classes)

add_library(cls_version_client
  version/cls_version_client.cc
  version/cls_version_types.cc)

# cls_log
add_library(cls_log SHARED log/cls_log.cc)
set_target_properties(cls_log PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS cls_log DESTINATION lib/rados-classes)

add_library(cls_log_client log/cls_log_client.cc)

# cls_statelog
add_library(cls_statelog SHARED statelog/cls_statelog.cc)
set_target_properties(cls_statelog PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS cls_statelog DESTINATION lib/rados-classes)

add_library(cls_statelog_client statelog/cls_statelog_client.cc)

# cls_timeindex
add_library(cls_timeindex SHARED timeindex/cls_timeindex.cc)
set_target_properties(cls_timeindex PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS cls_timeindex DESTINATION lib/rados-classes)

add_library(cls_timeindex_client timeindex/cls_timeindex_client.cc)

# cls_replica_log
add_library(cls_replica_log SHARED replica_log/cls_replica_log.cc)
set_target_properties(cls_replica_log PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS cls_replica_log DESTINATION lib/rados-classes)

add_library(cls_replica_log_client
  replica_log/cls_replica_log_types.cc
  replica_log/cls_replica_log_ops.cc
  replica_log/cls_replica_log_client.cc)

# cls_user
add_library(cls_user SHARED user/cls_user.cc)
set_target_properties(cls_user PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS cls_user DESTINATION lib/rados-classes)

add_library(cls_user_client
  user/cls_user_client.cc
  user/cls_user_types.cc
  user/cls_user_ops.cc)

# cls_journal
add_library(cls_journal SHARED
  journal/cls_journal.cc
  journal/cls_journal_types.cc)
set_target_properties(cls_journal PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS cls_journal DESTINATION lib/rados-classes)

add_library(cls_journal_client
  journal/cls_journal_client.cc
  journal/cls_journal_types.cc)

# cls_rgw
if (WITH_RADOSGW)
  add_library(cls_rgw SHARED
    rgw/cls_rgw.cc
    rgw/cls_rgw_ops.cc
    rgw/cls_rgw_types.cc
    ${CMAKE_SOURCE_DIR}/src/common/ceph_json.cc)
  target_link_libraries(cls_rgw json_spirit)
  set_target_properties(cls_rgw PROPERTIES VERSION "1.0.0" SOVERSION "1")
  install(TARGETS cls_rgw DESTINATION lib/rados-classes)

  add_library(cls_rgw_client
    rgw/cls_rgw_client.cc
    rgw/cls_rgw_types.cc
    rgw/cls_rgw_ops.cc)
endif (WITH_RADOSGW)

# cls_cephfs
if (WITH_CEPHFS)
  add_library(cls_cephfs SHARED
    cephfs/cls_cephfs.cc)
  set_target_properties(cls_cephfs PROPERTIES VERSION "1.0.0" SOVERSION "1")
  install(TARGETS cls_cephfs DESTINATION lib/rados-classes)

  add_library(cls_cephfs_client
    cephfs/cls_cephfs_client.cc)
endif (WITH_CEPHFS)

add_library(cls_numops_client STATIC numops/cls_numops_client.cc)

