## Rados object classes

set(cls_dir ${CMAKE_INSTALL_LIBDIR}/rados-classes)

# cls_sdk
add_library(cls_sdk SHARED sdk/cls_sdk.cc)
set_target_properties(cls_sdk PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_sdk DESTINATION ${cls_dir})

# cls_hello
set(cls_hello_srcs hello/cls_hello.cc)
add_library(cls_hello SHARED ${cls_hello_srcs})
set_target_properties(cls_hello PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_hello DESTINATION ${cls_dir})

# cls_numops
set(cls_numops_srcs numops/cls_numops.cc)
add_library(cls_numops SHARED ${cls_numops_srcs})
set_target_properties(cls_numops PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_numops DESTINATION ${cls_dir})

set(cls_numops_client_srcs numops/cls_numops_client.cc)
add_library(cls_numops_client STATIC ${cls_numops_client_srcs})


# cls_rbd
if (WITH_RBD)
  set(cls_rbd_srcs rbd/cls_rbd.cc rbd/cls_rbd_types.cc)
  add_library(cls_rbd SHARED ${cls_rbd_srcs})
  set_target_properties(cls_rbd PROPERTIES
    VERSION "1.0.0"
    SOVERSION "1"
    INSTALL_RPATH ""
    CXX_VISIBILITY_PRESET hidden)
  install(TARGETS cls_rbd DESTINATION ${cls_dir})

  set(cls_rbd_client_srcs rbd/cls_rbd_client.cc rbd/cls_rbd_types.cc)
  add_library(cls_rbd_client STATIC ${cls_rbd_client_srcs})
  target_link_libraries(cls_rbd_client cls_lock_client)

endif (WITH_RBD)

# cls_lock
set(cls_lock_srcs lock/cls_lock.cc)
add_library(cls_lock SHARED ${cls_lock_srcs})
set_target_properties(cls_lock PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_lock DESTINATION ${cls_dir})

set(cls_lock_client_srcs
  lock/cls_lock_client.cc
  lock/cls_lock_types.cc
  lock/cls_lock_ops.cc)
add_library(cls_lock_client STATIC ${cls_lock_client_srcs})


# cls_otp
if (WITH_RADOSGW)
  set(cls_otp_srcs otp/cls_otp.cc)
  add_library(cls_otp SHARED ${cls_otp_srcs})
  target_link_libraries(cls_otp OATH::OATH)
  target_include_directories(cls_otp
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw/driver/rados"
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/spawn/include")
  set_target_properties(cls_otp PROPERTIES
    VERSION "1.0.0"
    SOVERSION "1"
    INSTALL_RPATH ""
    CXX_VISIBILITY_PRESET hidden)
  install(TARGETS cls_otp DESTINATION ${cls_dir})

  set(cls_otp_client_srcs
    otp/cls_otp_client.cc
    otp/cls_otp_types.cc
    )
  add_library(cls_otp_client STATIC ${cls_otp_client_srcs})
endif (WITH_RADOSGW)

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
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_refcount DESTINATION ${cls_dir})

set(cls_refcount_client_srcs
  refcount/cls_refcount_client.cc
  refcount/cls_refcount_ops.cc)
add_library(cls_refcount_client STATIC ${cls_refcount_client_srcs})


# cls_version
set(cls_version_srcs version/cls_version.cc)
add_library(cls_version SHARED ${cls_version_srcs})
set_target_properties(cls_version PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_version DESTINATION ${cls_dir})

set(cls_version_client_srcs
  version/cls_version_client.cc
  version/cls_version_types.cc)
add_library(cls_version_client STATIC ${cls_version_client_srcs})


# cls_log
set(cls_log_srcs log/cls_log.cc)
add_library(cls_log SHARED ${cls_log_srcs})
set_target_properties(cls_log PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_log DESTINATION ${cls_dir})

set(cls_log_client_srcs log/cls_log_client.cc)
add_library(cls_log_client STATIC ${cls_log_client_srcs})


# cls_timeindex
set(cls_timeindex_srcs timeindex/cls_timeindex.cc)
add_library(cls_timeindex SHARED ${cls_timeindex_srcs})
set_target_properties(cls_timeindex PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_timeindex DESTINATION ${cls_dir})

set(cls_timeindex_client_srcs
  timeindex/cls_timeindex_types.cc
  timeindex/cls_timeindex_client.cc)
add_library(cls_timeindex_client STATIC ${cls_timeindex_client_srcs})


# cls_user
set(cls_user_srcs user/cls_user.cc)
add_library(cls_user SHARED ${cls_user_srcs})
set_target_properties(cls_user PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_user DESTINATION ${cls_dir})

set(cls_user_client_srcs
  user/cls_user_client.cc
  user/cls_user_types.cc
  user/cls_user_ops.cc)
add_library(cls_user_client STATIC ${cls_user_client_srcs})


# cls_journal
set(cls_journal_srcs
  journal/cls_journal.cc
  journal/cls_journal_types.cc)
add_library(cls_journal SHARED ${cls_journal_srcs})
set_target_properties(cls_journal PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_journal DESTINATION ${cls_dir})

set(cls_journal_client_srcs
  journal/cls_journal_client.cc
  journal/cls_journal_types.cc)
add_library(cls_journal_client STATIC ${cls_journal_client_srcs})


# cls_rgw
if (WITH_RADOSGW)
  set(cls_rgw_srcs
    rgw/cls_rgw.cc
    rgw/cls_rgw_ops.cc
    rgw/cls_rgw_types.cc
    ${CMAKE_SOURCE_DIR}/src/common/ceph_json.cc)
  add_library(cls_rgw SHARED ${cls_rgw_srcs})
  target_link_libraries(cls_rgw ${FMT_LIB} json_spirit)
  target_include_directories(cls_rgw
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw/driver/rados"
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/spawn/include")
  set_target_properties(cls_rgw PROPERTIES
    VERSION "1.0.0"
    SOVERSION "1"
    INSTALL_RPATH ""
    CXX_VISIBILITY_PRESET hidden)
  install(TARGETS cls_rgw DESTINATION ${cls_dir})

  set(cls_rgw_client_srcs
    rgw/cls_rgw_client.cc
    rgw/cls_rgw_types.cc
    rgw/cls_rgw_ops.cc)
  add_library(cls_rgw_client STATIC ${cls_rgw_client_srcs})
  target_include_directories(cls_rgw_client
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw/driver/rados"
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/spawn/include")

endif (WITH_RADOSGW)

# cls_cephfs
if (WITH_CEPHFS)
  set(cls_cephfs_srcs
    cephfs/cls_cephfs.cc)
  add_library(cls_cephfs SHARED ${cls_cephfs_srcs})
  set_target_properties(cls_cephfs PROPERTIES
    VERSION "1.0.0"
    SOVERSION "1"
    INSTALL_RPATH ""
    CXX_VISIBILITY_PRESET hidden)
  install(TARGETS cls_cephfs DESTINATION ${cls_dir})

  set(cls_cephfs_client_srcs
    cephfs/cls_cephfs_client.cc)
  add_library(cls_cephfs_client STATIC ${cls_cephfs_client_srcs})

endif (WITH_CEPHFS)

if (NOT WIN32)
  # cls_lua
  set(cls_lua_srcs
      lua/cls_lua.cc
      lua/lua_bufferlist.cc)
  add_library(cls_lua SHARED ${cls_lua_srcs})
  set_target_properties(cls_lua PROPERTIES
    VERSION "1.0.0"
    SOVERSION "1"
    INSTALL_RPATH ""
    CXX_VISIBILITY_PRESET hidden)
  install(TARGETS cls_lua DESTINATION ${cls_dir})
  target_link_libraries(cls_lua
      ${LUA_LIBRARIES}
      json_spirit)
  target_include_directories(cls_lua PRIVATE "${LUA_INCLUDE_DIR}")
endif (NOT WIN32)

set(cls_lua_client_srcs
    lua/cls_lua_client.cc)
add_library(cls_lua_client STATIC ${cls_lua_client_srcs})

# cls_cas
set(cls_cas_client_srcs
  cas/cls_cas_client.cc)
add_library(cls_cas_client STATIC ${cls_cas_client_srcs})

set(cls_cas_internal_srcs
  cas/cls_cas_internal.cc)
add_library(cls_cas_internal STATIC ${cls_cas_internal_srcs})

set(cls_cas_srcs 
  cas/cls_cas.cc)
add_library(cls_cas SHARED ${cls_cas_srcs})
target_link_libraries(cls_cas cls_cas_internal)
set_target_properties(cls_cas PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_cas DESTINATION ${cls_dir})



#cls_queue
set(cls_queue_srcs
  queue/cls_queue.cc
  queue/cls_queue_src.cc
  ${CMAKE_SOURCE_DIR}/src/common/ceph_json.cc)
add_library(cls_queue SHARED ${cls_queue_srcs})
set_target_properties(cls_queue PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_queue DESTINATION ${cls_dir})

set(cls_queue_client_srcs
  queue/cls_queue_client.cc)
add_library(cls_queue_client STATIC ${cls_queue_client_srcs})

# cls_rgw_gc
if (WITH_RADOSGW)
  set(cls_rgw_gc_srcs
    rgw_gc/cls_rgw_gc.cc
    queue/cls_queue_src.cc
    ${CMAKE_SOURCE_DIR}/src/common/ceph_json.cc)
  add_library(cls_rgw_gc SHARED ${cls_rgw_gc_srcs})
  target_include_directories(cls_rgw_gc
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw/driver/rados"
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/spawn/include")
  set_target_properties(cls_rgw_gc PROPERTIES
    VERSION "1.0.0"
    SOVERSION "1"
    INSTALL_RPATH ""
    CXX_VISIBILITY_PRESET hidden)
  install(TARGETS cls_rgw_gc DESTINATION ${cls_dir})

  set(cls_rgw_gc_client_srcs
    rgw_gc/cls_rgw_gc_client.cc
    rgw/cls_rgw_types.cc)
  add_library(cls_rgw_gc_client STATIC ${cls_rgw_gc_client_srcs})
  target_include_directories(cls_rgw_gc_client
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw/driver/rados"
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
	  PUBLIC "${CMAKE_SOURCE_DIR}/src/spawn/include")
endif (WITH_RADOSGW)


#cls_2pc_queue
set(cls_2pc_queue_srcs
  2pc_queue/cls_2pc_queue.cc
  queue/cls_queue_src.cc
  ${CMAKE_SOURCE_DIR}/src/common/ceph_json.cc)
add_library(cls_2pc_queue SHARED ${cls_2pc_queue_srcs})
set_target_properties(cls_2pc_queue PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_2pc_queue DESTINATION ${cls_dir})
set(cls_2pc_queue_client_srcs
  2pc_queue/cls_2pc_queue_client.cc)
add_library(cls_2pc_queue_client STATIC ${cls_2pc_queue_client_srcs})


add_subdirectory(cmpomap)

# cls_fifo
set(cls_fifo_srcs fifo/cls_fifo.cc)
add_library(cls_fifo SHARED ${cls_fifo_srcs})
set_target_properties(cls_fifo PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
target_link_libraries(cls_fifo ${FMT_LIB})
install(TARGETS cls_fifo DESTINATION ${cls_dir})

# cls_test_remote_reads
set(cls_test_remote_reads_srcs test_remote_reads/cls_test_remote_reads.cc)
add_library(cls_test_remote_reads SHARED ${cls_test_remote_reads_srcs})
set_target_properties(cls_test_remote_reads PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_test_remote_reads DESTINATION ${cls_dir})
