## dencoder
set_source_files_properties(
  ceph_dencoder.cc
  APPEND PROPERTY OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h)

if(HAS_VTA)
  set_source_files_properties(ceph_dencoder.cc
    PROPERTIES COMPILE_FLAGS -fno-var-tracking-assignments)
endif()

set(dencoder_srcs
  ceph_dencoder.cc
  $<TARGET_OBJECTS:common_texttable_obj>)
if(WITH_RADOSGW)
  list(APPEND dencoder_srcs
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_dencoder.cc)
endif()

add_executable(ceph-dencoder ${dencoder_srcs})

if(WITH_RADOSGW)
  list(APPEND DENCODER_EXTRALIBS
    rgw_a
    cls_rgw_client)
  if(WITH_RADOSGW_AMQP_ENDPOINT)
    list(APPEND DENCODER_EXTRALIBS
      rabbitmq)
  endif()
endif()

if(WITH_RBD)
  list(APPEND DENCODER_EXTRALIBS
    cls_rbd_client
    rbd_mirror_types
    rbd_types
    rbd_replay_types)
  if(WITH_KRBD)
    list(APPEND DENCODER_EXTRALIBS
      krbd)
  endif()
endif()

if(WITH_CEPHFS)
  list(APPEND DENCODER_EXTRALIBS
    mds)
endif()

target_link_libraries(ceph-dencoder
  global
  os
  osd
  mon
  journal
  ${DENCODER_EXTRALIBS}
  cls_lock_client
  cls_refcount_client
  cls_log_client
  cls_version_client
  cls_user_client
  cls_journal_client
  cls_timeindex_client
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS})
install(TARGETS ceph-dencoder DESTINATION bin)
