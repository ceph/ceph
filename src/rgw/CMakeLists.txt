if(WITH_TESTS)
add_executable(ceph_rgw_jsonparser
  rgw_jsonparser.cc)
target_link_libraries(ceph_rgw_jsonparser
  rgw_a
  global)

add_executable(ceph_rgw_multiparser
  rgw_multiparser.cc)
target_link_libraries(ceph_rgw_multiparser
  rgw_a
  global)

install(TARGETS
  ceph_rgw_jsonparser
  ceph_rgw_multiparser
  DESTINATION bin)
endif(WITH_TESTS)


set(rgw_a_srcs
  rgw_acl.cc
  rgw_acl_s3.cc
  rgw_acl_swift.cc
  rgw_auth.cc
  rgw_auth_s3.cc
  rgw_basic_types.cc
  rgw_bucket.cc
  rgw_cache.cc
  rgw_client_io.cc
  rgw_common.cc
  rgw_cors.cc
  rgw_cors_s3.cc
  rgw_dencoder.cc
  rgw_env.cc
  rgw_fcgi.cc
  rgw_formats.cc
  rgw_frontend.cc
  rgw_gc.cc
  rgw_http_client.cc
  rgw_json_enc.cc
  rgw_keystone.cc
  rgw_ldap.cc
  rgw_loadgen.cc
  rgw_log.cc
  rgw_lc.cc
  rgw_lc_s3.cc
  rgw_metadata.cc
  rgw_multi.cc
  rgw_multi_del.cc
  rgw_sync.cc
  rgw_data_sync.cc
  rgw_sync_module.cc
  rgw_sync_module_es.cc
  rgw_sync_module_log.cc
  rgw_period_history.cc
  rgw_period_puller.cc
  rgw_period_pusher.cc
  rgw_realm_reloader.cc
  rgw_realm_watcher.cc
  rgw_coroutine.cc
  rgw_cr_rados.cc
  rgw_object_expirer_core.cc
  rgw_op.cc
  rgw_os_lib.cc
  rgw_policy_s3.cc
  rgw_process.cc
  rgw_quota.cc
  rgw_rados.cc
  rgw_replica_log.cc
  rgw_request.cc
  rgw_resolve.cc
  rgw_rest_bucket.cc
  rgw_rest.cc
  rgw_rest_client.cc
  rgw_rest_config.cc
  rgw_rest_conn.cc
  rgw_rest_log.cc
  rgw_rest_metadata.cc
  rgw_rest_opstate.cc
  rgw_rest_realm.cc
  rgw_rest_replica_log.cc
  rgw_rest_s3.cc
  rgw_rest_swift.cc
  rgw_rest_usage.cc
  rgw_rest_user.cc
  rgw_swift_auth.cc
  rgw_tools.cc
  rgw_usage.cc
  rgw_user.cc
  rgw_website.cc
  rgw_xml.cc
  rgw_xml_enc.cc
  rgw_torrent.cc)

add_library(rgw_a STATIC ${rgw_a_srcs})

target_include_directories(rgw_a PUBLIC
  "../Beast/include"
  ${FCGI_INCLUDE_DIR})

target_link_libraries(rgw_a librados cls_lock_client cls_rgw_client cls_refcount_client
  cls_log_client cls_statelog_client cls_timeindex_client cls_version_client
  cls_replica_log_client cls_user_client common common_utf8 global
  ${CURL_LIBRARIES}
  ${EXPAT_LIBRARIES}
  ${OPENLDAP_LIBRARIES} ${CRYPTO_LIBS})

set(radosgw_srcs
  rgw_fcgi_process.cc
  rgw_loadgen_process.cc
  rgw_civetweb.cc
  rgw_civetweb_frontend.cc
  rgw_civetweb_log.cc
  rgw_main.cc)

if (WITH_RADOSGW_ASIO_FRONTEND)
  list(APPEND radosgw_srcs
    rgw_asio_client.cc
    rgw_asio_frontend.cc)
endif (WITH_RADOSGW_ASIO_FRONTEND)

add_executable(radosgw ${radosgw_srcs}  $<TARGET_OBJECTS:civetweb_common_objs>)
target_link_libraries(radosgw rgw_a librados
  cls_rgw_client cls_lock_client cls_refcount_client
  cls_log_client cls_statelog_client cls_timeindex_client
  cls_version_client cls_replica_log_client cls_user_client
  global fcgi ${LIB_RESOLV}
  ${CURL_LIBRARIES} ${EXPAT_LIBRARIES} ${SSL_LIBRARIES} ${BLKID_LIBRARIES}
  ${ALLOC_LIBS})
# radosgw depends on cls libraries at runtime, but not as link dependencies
add_dependencies(radosgw cls_rgw cls_lock cls_refcount
  cls_log cls_statelog cls_timeindex
  cls_version cls_replica_log cls_user)
install(TARGETS radosgw DESTINATION bin)

set(radosgw_admin_srcs
  rgw_admin.cc
  rgw_orphan.cc)
add_executable(radosgw-admin ${radosgw_admin_srcs})
target_link_libraries(radosgw-admin rgw_a librados
  cls_rgw_client cls_lock_client cls_refcount_client
  cls_log_client cls_statelog_client cls_timeindex_client
  cls_version_client cls_replica_log_client cls_user_client
  global fcgi ${LIB_RESOLV}
  ${CURL_LIBRARIES} ${EXPAT_LIBRARIES} ${SSL_LIBRARIES} ${BLKID_LIBRARIES})
install(TARGETS radosgw-admin DESTINATION bin)

set(radosgw_token_srcs
  rgw_token.cc)
add_executable(radosgw-token ${radosgw_token_srcs})
target_link_libraries(radosgw-token librados
  global ${ALLOC_LIBS})
install(TARGETS radosgw-token DESTINATION bin)

set(radosgw_object_expirer_srcs
  rgw_object_expirer.cc)
add_executable(radosgw-object-expirer ${radosgw_object_expirer_srcs})
target_link_libraries(radosgw-object-expirer rgw_a librados
  cls_rgw_client cls_lock_client cls_refcount_client
  cls_log_client cls_statelog_client cls_timeindex_client
  cls_version_client cls_replica_log_client cls_user_client
  global fcgi ${LIB_RESOLV}
  ${CURL_LIBRARIES} ${EXPAT_LIBRARIES})
install(TARGETS radosgw-object-expirer DESTINATION bin)

set(librgw_srcs
  librgw.cc
  rgw_file.cc)
add_library(rgw SHARED ${librgw_srcs})
target_link_libraries(rgw LINK_PRIVATE
  rgw_a
  librados
  cls_rgw_client
  cls_lock_client
  cls_refcount_client
  cls_log_client
  cls_statelog_client
  cls_timeindex_client
  cls_version_client
  cls_replica_log_client
  cls_user_client
  global
  ${LIB_RESOLV}
  ${CURL_LIBRARIES}
  ${EXPAT_LIBRARIES})
set_target_properties(rgw PROPERTIES OUTPUT_NAME rgw VERSION 2.0.0
  SOVERSION 2)
install(TARGETS rgw DESTINATION ${CMAKE_INSTALL_LIBDIR})
