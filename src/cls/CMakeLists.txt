## Rados object classes

set(cls_dir ${CMAKE_INSTALL_LIBDIR}/rados-classes)
set(cls_embedded_srcs)

# cls_sdk
add_library(cls_sdk SHARED sdk/cls_sdk.cc)
set_target_properties(cls_sdk PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_sdk DESTINATION ${cls_dir})

# cls_hello
set(cls_hello_srcs hello/cls_hello.cc)
add_library(cls_hello SHARED ${cls_hello_srcs})
set_target_properties(cls_hello PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_hello DESTINATION ${cls_dir})
list(APPEND cls_embedded_srcs ${cls_hello_srcs})

# cls_numops
set(cls_numops_srcs numops/cls_numops.cc)
add_library(cls_numops SHARED ${cls_numops_srcs})
set_target_properties(cls_numops PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_numops DESTINATION ${cls_dir})

set(cls_numops_client_srcs numops/cls_numops_client.cc)
add_library(cls_numops_client STATIC ${cls_numops_client_srcs})

list(APPEND cls_embedded_srcs ${cls_numops_srcs} ${cls_numops_client_srcs})

# cls_rbd
if (WITH_RBD)
  set(cls_rbd_srcs rbd/cls_rbd.cc rbd/cls_rbd_types.cc)
  add_library(cls_rbd SHARED ${cls_rbd_srcs})
  set_target_properties(cls_rbd PROPERTIES
    VERSION "1.0.0"
    SOVERSION "1"
    INSTALL_RPATH "")
  install(TARGETS cls_rbd DESTINATION ${cls_dir})

  set(cls_rbd_client_srcs rbd/cls_rbd_client.cc rbd/cls_rbd_types.cc)
  add_library(cls_rbd_client STATIC ${cls_rbd_client_srcs})
  target_link_libraries(cls_rbd_client cls_lock_client)

  list(APPEND cls_embedded_srcs ${cls_rbd_srcs} ${cls_rbd_client_srcs})
endif (WITH_RBD)

# cls_lock
set(cls_lock_srcs lock/cls_lock.cc)
add_library(cls_lock SHARED ${cls_lock_srcs})
set_target_properties(cls_lock PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_lock DESTINATION ${cls_dir})

set(cls_lock_client_srcs
  lock/cls_lock_client.cc
  lock/cls_lock_types.cc
  lock/cls_lock_ops.cc)
add_library(cls_lock_client STATIC ${cls_lock_client_srcs})

list(APPEND cls_embedded_srcs ${cls_lock_srcs} ${cls_lock_client_srcs})

# cls_refcount
set(cls_refcount_srcs
  refcount/cls_refcount.cc
  refcount/cls_refcount_ops.cc
  ${CMAKE_SOURCE_DIR}/src/common/ceph_json.cc)
add_library(cls_refcount SHARED ${cls_refcount_srcs})
target_link_libraries(cls_refcount json_spirit)
set_target_properties(cls_refcount PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_refcount DESTINATION ${cls_dir})

set(cls_refcount_client_srcs
  refcount/cls_refcount_client.cc
  refcount/cls_refcount_ops.cc)
add_library(cls_refcount_client STATIC ${cls_refcount_client_srcs})

list(APPEND cls_embedded_srcs ${cls_refcount_srcs} ${cls_refcount_client_srcs})

# cls_version
set(cls_version_srcs version/cls_version.cc)
add_library(cls_version SHARED ${cls_version_srcs})
set_target_properties(cls_version PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_version DESTINATION ${cls_dir})

set(cls_version_client_srcs
  version/cls_version_client.cc
  version/cls_version_types.cc)
add_library(cls_version_client STATIC ${cls_version_client_srcs})

list(APPEND cls_embedded_srcs ${cls_version_srcs} ${cls_version_client_srcs})

# cls_log
set(cls_log_srcs log/cls_log.cc)
add_library(cls_log SHARED ${cls_log_srcs})
set_target_properties(cls_log PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_log DESTINATION ${cls_dir})

set(cls_log_client_srcs log/cls_log_client.cc)
add_library(cls_log_client STATIC ${cls_log_client_srcs})

list(APPEND cls_embedded_srcs ${cls_log_srcs} ${cls_log_client_srcs})

# cls_statelog
set(cls_statelog_srcs statelog/cls_statelog.cc)
add_library(cls_statelog SHARED ${cls_statelog_srcs})
set_target_properties(cls_statelog PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_statelog DESTINATION ${cls_dir})

set(cls_statelog_client_srcs statelog/cls_statelog_client.cc)
add_library(cls_statelog_client STATIC ${cls_statelog_client_srcs})

list(APPEND cls_embedded_srcs ${cls_statelog_srcs} ${cls_statelog_client_srcs})

# cls_timeindex
set(cls_timeindex_srcs timeindex/cls_timeindex.cc)
add_library(cls_timeindex SHARED ${cls_timeindex_srcs})
set_target_properties(cls_timeindex PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_timeindex DESTINATION ${cls_dir})

set(cls_timeindex_client_srcs timeindex/cls_timeindex_client.cc)
add_library(cls_timeindex_client STATIC ${cls_timeindex_client_srcs})

list(APPEND cls_embedded_srcs ${cls_timeindex_srcs} ${cls_timeindex_client_srcs})

# cls_replica_log
set(cls_replica_log_srcs replica_log/cls_replica_log.cc)
add_library(cls_replica_log SHARED ${cls_replica_log_srcs})
set_target_properties(cls_replica_log PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_replica_log DESTINATION ${cls_dir})

set(cls_replica_log_client_srcs
  replica_log/cls_replica_log_types.cc
  replica_log/cls_replica_log_ops.cc
  replica_log/cls_replica_log_client.cc)
add_library(cls_replica_log_client STATIC ${cls_replica_log_client_srcs})

list(APPEND cls_embedded_srcs ${cls_replica_log_srcs} ${cls_replica_log_client_srcs})

# cls_user
set(cls_user_srcs user/cls_user.cc)
add_library(cls_user SHARED ${cls_user_srcs})
set_target_properties(cls_user PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_user DESTINATION ${cls_dir})

set(cls_user_client_srcs
  user/cls_user_client.cc
  user/cls_user_types.cc
  user/cls_user_ops.cc)
add_library(cls_user_client STATIC ${cls_user_client_srcs})

list(APPEND cls_embedded_srcs ${cls_user_srcs} ${cls_user_client_srcs})

# cls_journal
set(cls_journal_srcs
  journal/cls_journal.cc
  journal/cls_journal_types.cc)
add_library(cls_journal SHARED ${cls_journal_srcs})
set_target_properties(cls_journal PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_journal DESTINATION ${cls_dir})

set(cls_journal_client_srcs
  journal/cls_journal_client.cc
  journal/cls_journal_types.cc)
add_library(cls_journal_client STATIC ${cls_journal_client_srcs})

list(APPEND cls_embedded_srcs ${cls_journal_srcs} ${cls_journal_client_srcs})

# cls_rgw
if (WITH_RADOSGW)
  set(cls_rgw_srcs
    rgw/cls_rgw.cc
    rgw/cls_rgw_ops.cc
    rgw/cls_rgw_types.cc
    ${CMAKE_SOURCE_DIR}/src/common/ceph_json.cc)
  add_library(cls_rgw SHARED ${cls_rgw_srcs})
  target_link_libraries(cls_rgw json_spirit)
  set_target_properties(cls_rgw PROPERTIES
    VERSION "1.0.0"
    SOVERSION "1"
    INSTALL_RPATH "")
  install(TARGETS cls_rgw DESTINATION ${cls_dir})

  set(cls_rgw_client_srcs
    rgw/cls_rgw_client.cc
    rgw/cls_rgw_types.cc
    rgw/cls_rgw_ops.cc)
  add_library(cls_rgw_client STATIC ${cls_rgw_client_srcs})

  list(APPEND cls_embedded_srcs ${cls_rgw_srcs} ${cls_rgw_client_srcs})
endif (WITH_RADOSGW)

# cls_cephfs
if (WITH_CEPHFS)
  set(cls_cephfs_srcs
    cephfs/cls_cephfs.cc)
  add_library(cls_cephfs SHARED ${cls_cephfs_srcs})
  set_target_properties(cls_cephfs PROPERTIES
    VERSION "1.0.0"
    SOVERSION "1"
    INSTALL_RPATH "")
  install(TARGETS cls_cephfs DESTINATION ${cls_dir})

  set(cls_cephfs_client_srcs
    cephfs/cls_cephfs_client.cc)
  add_library(cls_cephfs_client STATIC ${cls_cephfs_client_srcs})

  list(APPEND cls_embedded_srcs ${cls_cephfs_srcs} ${cls_cephfs_client_srcs})
endif (WITH_CEPHFS)

# cls_lua
set(cls_lua_srcs
    lua/cls_lua.cc
    lua/lua_bufferlist.cc)
add_library(cls_lua SHARED ${cls_lua_srcs})
set_target_properties(cls_lua PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_lua DESTINATION ${cls_dir})
target_link_libraries(cls_lua
    liblua
    json_spirit)

set(cls_lua_client_srcs
    lua/cls_lua_client.cc)
add_library(cls_lua_client STATIC ${cls_lua_client_srcs})

list(APPEND cls_embedded_srcs ${cls_lua_srcs} ${cls_lua_client_srcs})

if(WITH_EMBEDDED)
  include(MergeStaticLibraries)
  list(REMOVE_DUPLICATES cls_embedded_srcs)
  add_library(cephd_cls_base STATIC ${cls_embedded_srcs})
  # while not necessary this seems to bring in the lua's include directories
  # so that cls_lua srcs build correctly
  target_link_libraries(cephd_cls_base liblua)
  set_target_properties(cephd_cls_base PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
  merge_static_libraries(cephd_cls cephd_cls_base liblua)
endif()
