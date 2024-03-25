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
  ../../include/uuid.cc
  ../../include/utime.cc
  $<TARGET_OBJECTS:common_texttable_obj>)
add_executable(ceph-dencoder ${dencoder_srcs})
set_target_properties(ceph-dencoder PROPERTIES
  JOB_POOL_COMPILE heavy_compile_job_pool
  JOB_POOL_LINK heavy_link_job_pool)

set(denc_plugin_dir ${CEPH_INSTALL_FULL_PKGLIBDIR}/denc)
add_custom_target(ceph-dencoder-modules)

function(add_denc_mod name)
  add_library(${name} SHARED
    ${ARGN})
  set_target_properties(${name} PROPERTIES
    PREFIX ""
    OUTPUT_NAME ${name}
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON)
  install(
    TARGETS ${name}
    DESTINATION ${denc_plugin_dir})
  add_dependencies(ceph-dencoder-modules
    ${name})
endfunction()

add_denc_mod(denc-mod-common
  common_types.cc)
target_link_libraries(denc-mod-common
  journal
  cls_cas_internal
  cls_lock_client
  cls_refcount_client
  cls_timeindex_client)
add_denc_mod(denc-mod-osd
  osd_types.cc)
target_link_libraries(denc-mod-osd
  os
  osd
  mon
  erasure_code
  global)

if(WITH_RADOSGW)
  add_denc_mod(denc-mod-rgw
    rgw_types.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_dencoder.cc)
  target_include_directories(denc-mod-rgw
    SYSTEM PRIVATE "${CMAKE_SOURCE_DIR}/src/rgw")
  target_link_libraries(denc-mod-rgw
    rgw_a
    cls_rgw_client
    cls_journal_client)
  if(WITH_RADOSGW_AMQP_ENDPOINT)
    target_link_libraries(denc-mod-rgw
      rabbitmq ssl)
  endif()
  if(WITH_RADOSGW_KAFKA_ENDPOINT)
    target_link_libraries(denc-mod-rgw
      rdkafka)
  endif()
endif()

if(WITH_RBD)
  add_denc_mod(denc-mod-rbd
    rbd_types.cc)
  target_link_libraries(denc-mod-rbd
    cls_rbd_client
    rbd_mirror_types
    rbd_types
    rbd_replay_types)
  if(WITH_KRBD)
    target_link_libraries(denc-mod-rbd
      krbd)
  endif()
endif()

if(WITH_CEPHFS)
  add_denc_mod(denc-mod-cephfs
    mds_types.cc)
  target_link_libraries(denc-mod-cephfs
    mds)
endif()

target_compile_definitions(ceph-dencoder PRIVATE
  "CEPH_DENC_MOD_DIR=\"${denc_plugin_dir}\"")

target_link_libraries(ceph-dencoder
  StdFilesystem::filesystem
  global
  ${DENCODER_EXTRALIBS}
  cls_log_client
  cls_version_client
  cls_user_client
  cls_cas_client
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS})
install(TARGETS ceph-dencoder DESTINATION bin)
