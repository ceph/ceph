find_program(GPERF gperf)
if(NOT GPERF)
  message(FATAL_ERROR "Can't find gperf")
endif()
function(gperf_generate input output)
  add_custom_command(
    OUTPUT ${output}
    COMMAND ${GPERF} ${input} | sed "s/register //g" > ${output}
    DEPENDS ${input}
    COMMENT "Generate ${output}"
    )
endfunction()

find_package(ICU 52.0 COMPONENTS uc REQUIRED)

set(librgw_common_srcs
  services/svc_finisher.cc
  services/svc_bi_rados.cc
  services/svc_bilog_rados.cc
  services/svc_bucket.cc
  services/svc_bucket_sobj.cc
  services/svc_bucket_sync_sobj.cc
  services/svc_cls.cc
  services/svc_config_key_rados.cc
  services/svc_mdlog.cc
  services/svc_meta.cc
  services/svc_meta_be.cc
  services/svc_meta_be_otp.cc
  services/svc_meta_be_sobj.cc
  services/svc_notify.cc
  services/svc_otp.cc
  services/svc_quota.cc
  services/svc_sync_modules.cc
  services/svc_rados.cc
  services/svc_sys_obj.cc
  services/svc_sys_obj_cache.cc
  services/svc_sys_obj_core.cc
  services/svc_tier_rados.cc
  services/svc_user.cc
  services/svc_user_rados.cc
  services/svc_zone.cc
  services/svc_zone_utils.cc
  rgw_service.cc
  rgw_acl.cc
  rgw_acl_s3.cc
  rgw_acl_swift.cc
  rgw_aio.cc
  rgw_aio_throttle.cc
  rgw_auth.cc
  rgw_auth_s3.cc
  rgw_arn.cc
  rgw_basic_types.cc
  rgw_bucket.cc
  rgw_bucket_layout.cc
  rgw_bucket_sync.cc
  rgw_cache.cc
  rgw_d3n_datacache.cc
  rgw_common.cc
  rgw_compression.cc
  rgw_etag_verifier.cc
  rgw_cors.cc
  rgw_cors_s3.cc
  rgw_env.cc
  rgw_es_query.cc
  rgw_formats.cc
  rgw_gc.cc
  rgw_gc_log.cc
  rgw_http_client.cc
  rgw_keystone.cc
  rgw_ldap.cc
  rgw_lc.cc
  rgw_lc_s3.cc
  rgw_lc_tier.cc
  rgw_metadata.cc
  rgw_multi.cc
  rgw_multi_del.cc
  rgw_obj_manifest.cc
  rgw_pubsub.cc
  rgw_sync.cc
  rgw_data_sync.cc
  rgw_sync_counters.cc
  rgw_sync_error_repo.cc
  rgw_sync_module.cc
  rgw_sync_module_aws.cc
  rgw_sync_module_es.cc
  rgw_sync_module_es_rest.cc
  rgw_sync_module_log.cc
  rgw_sync_module_pubsub.cc
  rgw_sync_policy.cc
  rgw_pubsub_push.cc
  rgw_notify.cc
  rgw_notify_event_type.cc
  rgw_sync_module_pubsub_rest.cc
  rgw_sync_trace.cc
  rgw_trim_bilog.cc
  rgw_trim_datalog.cc
  rgw_trim_mdlog.cc
  rgw_period_history.cc
  rgw_period_puller.cc
  rgw_reshard.cc
  rgw_coroutine.cc
  rgw_cr_rados.cc
  rgw_cr_rest.cc
  rgw_cr_tools.cc
  rgw_object_expirer_core.cc
  rgw_op.cc
  rgw_otp.cc
  rgw_policy_s3.cc
  rgw_public_access.cc
  rgw_putobj.cc
  rgw_putobj_processor.cc
  rgw_quota.cc
  rgw_rados.cc
  rgw_resolve.cc
  rgw_rest.cc
  rgw_rest_client.cc
  rgw_rest_conn.cc
  rgw_rest_log.cc
  rgw_rest_metadata.cc
  rgw_rest_pubsub.cc
  rgw_rest_pubsub_common.cc
  rgw_rest_realm.cc
  rgw_rest_role.cc
  rgw_rest_s3.cc
  rgw_role.cc
  rgw_sal.cc
  rgw_sal_rados.cc
  rgw_string.cc
  rgw_tag.cc
  rgw_tag_s3.cc
  rgw_tools.cc
  rgw_log_backing.cc
  rgw_user.cc
  rgw_website.cc
  rgw_xml.cc
  rgw_torrent.cc
  rgw_crypt.cc
  rgw_crypt_sanitize.cc
  rgw_iam_policy.cc
  rgw_rest_user_policy.cc
  rgw_zone.cc
  rgw_sts.cc
  rgw_rest_sts.cc
  rgw_perf_counters.cc
  rgw_rest_oidc_provider.cc
  rgw_rest_iam.cc
  rgw_object_lock.cc
  rgw_kms.cc
  rgw_kmip_client.cc
  rgw_url.cc
  rgw_oidc_provider.cc
  rgw_datalog.cc
  cls_fifo_legacy.cc
  rgw_lua_utils.cc
  rgw_lua.cc
  rgw_lua_request.cc
  rgw_bucket_encryption.cc
  rgw_tracer.cc)

if(WITH_RADOSGW_AMQP_ENDPOINT)
  list(APPEND librgw_common_srcs rgw_amqp.cc)
endif()
if(WITH_RADOSGW_KAFKA_ENDPOINT)
  list(APPEND librgw_common_srcs rgw_kafka.cc)
endif()
if(WITH_RADOSGW_DBSTORE)
  add_subdirectory(store/dbstore)
  list(APPEND librgw_common_srcs rgw_sal_dbstore.cc)
endif()
if(WITH_RADOSGW_MOTR)
  list(APPEND librgw_common_srcs rgw_sal_motr.cc)
endif()
if(WITH_JAEGER)
  list(APPEND librgw_common_srcs rgw_tracer.cc)
endif()

add_library(rgw_common STATIC ${librgw_common_srcs})

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-Wimplicit-const-int-float-conversion"
  COMPILER_SUPPORTS_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION)
if(COMPILER_SUPPORTS_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION)
  target_compile_definitions(common-objs PRIVATE
    HAVE_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION)
endif()

target_link_libraries(rgw_common
  PRIVATE
    ceph-common
    cls_2pc_queue_client
    cls_cmpomap_client
    cls_lock_client
    cls_log_client
    cls_otp_client
    cls_refcount_client
    cls_rgw_client
    cls_rgw_gc_client
    cls_timeindex_client
    cls_user_client
    cls_version_client
    librados
    rt
    fmt::fmt
    ICU::uc
    OATH::OATH
    dmclock::dmclock
    ${CURL_LIBRARIES}
    ${EXPAT_LIBRARIES}
    ${LUA_LIBRARIES}
  PUBLIC
    spawn)
target_include_directories(rgw_common
  SYSTEM PUBLIC "services"
  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
  PRIVATE "${LUA_INCLUDE_DIR}")
if(WITH_RADOSGW_KAFKA_ENDPOINT)
  # used by rgw_kafka.cc
  target_link_libraries(rgw_common
    PRIVATE
      RDKafka::RDKafka)
endif()
if(WITH_RADOSGW_AMQP_ENDPOINT)
  # used by rgw_amqp.cc
  target_link_libraries(rgw_common
    PRIVATE
      RabbitMQ::RabbitMQ
      OpenSSL::SSL)
endif()
if(WITH_OPENLDAP)
  target_link_libraries(rgw_common
    PRIVATE
      OpenLDAP::OpenLDAP)
endif()
if(WITH_RADOSGW_LUA_PACKAGES)
  target_link_libraries(rgw_common
    PRIVATE Boost::filesystem StdFilesystem::filesystem)
endif()

if(WITH_LTTNG)
  # rgw/rgw_op.cc includes "tracing/rgw_op.h"
  # rgw/rgw_rados.cc includes "tracing/rgw_rados.h"
  add_dependencies(rgw_common rgw_op-tp rgw_rados-tp)
endif()

if(WITH_JAEGER)
  add_dependencies(rgw_common ${jaeger_base})
  target_link_libraries(rgw_common PUBLIC ${jaeger_base})
endif()

if(WITH_RADOSGW_DBSTORE)
  target_link_libraries(rgw_common PRIVATE global dbstore)
endif()

if(WITH_RADOSGW_MOTR)
  target_include_directories(rgw_common PRIVATE "/usr/include/motr")
  target_compile_options(rgw_common
    PRIVATE "-Wno-attributes")
  target_compile_definitions(rgw_common
    PRIVATE "M0_EXTERN=extern" "M0_INTERNAL=")
  target_link_libraries(rgw_common PRIVATE "motr")
endif()

set(rgw_a_srcs
  rgw_auth_keystone.cc
  rgw_client_io.cc
  rgw_frontend.cc
  rgw_http_client_curl.cc
  rgw_loadgen.cc
  rgw_log.cc
  rgw_period_pusher.cc
  rgw_realm_reloader.cc
  rgw_realm_watcher.cc
  rgw_os_lib.cc
  rgw_process.cc
  rgw_rest_bucket.cc
  rgw_rest_config.cc
  rgw_rest_log.cc
  rgw_rest_metadata.cc
  rgw_rest_realm.cc
  rgw_rest_swift.cc
  rgw_rest_usage.cc
  rgw_rest_info.cc
  rgw_rest_user.cc
  rgw_swift_auth.cc
  rgw_usage.cc
  rgw_opa.cc
  rgw_sts.cc
  rgw_rest_sts.cc)

gperf_generate(${CMAKE_SOURCE_DIR}/src/rgw/rgw_iam_policy_keywords.gperf
  rgw_iam_policy_keywords.frag.cc)
set_source_files_properties(rgw_iam_policy.cc PROPERTIES
  OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/src/rgw/rgw_iam_policy_keywords.frag.cc
  COMPILE_FLAGS -I${CMAKE_BINARY_DIR}/src/rgw)


add_library(rgw_a STATIC
    ${rgw_a_srcs})

target_compile_definitions(rgw_a PUBLIC "-DCLS_CLIENT_HIDE_IOCTX")
target_include_directories(rgw_a PUBLIC "${CMAKE_SOURCE_DIR}/src/dmclock/support/src")
target_include_directories(rgw_a SYSTEM PUBLIC "../rapidjson/include")
target_include_directories(rgw_a SYSTEM PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw")

if(WITH_RADOSGW_AMQP_ENDPOINT)
  find_package(RabbitMQ REQUIRED)
endif()
if(WITH_RADOSGW_KAFKA_ENDPOINT)
  find_package(RDKafka 0.9.2 REQUIRED)
endif()

target_link_libraries(rgw_a
  PRIVATE
    common_utf8 global
    ${CRYPTO_LIBS}
    ${LUA_LIBRARIES}
    OATH::OATH
  PUBLIC
    rgw_common
    spawn)

if(WITH_CURL_OPENSSL)
  # used by rgw_http_client_curl.cc
  target_link_libraries(rgw_a PRIVATE OpenSSL::Crypto)
endif()

set(rgw_libs rgw_a)

list(APPEND rgw_libs ${LUA_LIBRARIES})

set(rgw_schedulers_srcs
  rgw_dmclock_scheduler_ctx.cc
  rgw_dmclock_sync_scheduler.cc
  rgw_dmclock_async_scheduler.cc)
set(radosgw_srcs
  rgw_loadgen_process.cc
  rgw_asio_client.cc
  rgw_asio_frontend.cc)

add_library(rgw_schedulers STATIC ${rgw_schedulers_srcs})
target_link_libraries(rgw_schedulers
  PUBLIC dmclock::dmclock spawn)

add_library(radosgw SHARED
  ${radosgw_srcs}
  ${rgw_a_srcs}
  rgw_main.cc
  rgw_kmip_client_impl.cc)

target_compile_definitions(radosgw PUBLIC "-DCLS_CLIENT_HIDE_IOCTX")
target_include_directories(radosgw
  PUBLIC "${CMAKE_SOURCE_DIR}/src/dmclock/support/src"
  PRIVATE "${CMAKE_SOURCE_DIR}/src/libkmip")
target_include_directories(radosgw SYSTEM PUBLIC "../rapidjson/include")

target_link_libraries(radosgw
  PRIVATE ${rgw_libs} rgw_schedulers kmip
  PUBLIC dmclock::dmclock
)
if(WITH_RADOSGW_BEAST_OPENSSL)
  # used by rgw_asio_frontend.cc
  target_link_libraries(radosgw PRIVATE OpenSSL::SSL)
endif()
set_target_properties(radosgw PROPERTIES OUTPUT_NAME radosgw VERSION 2.0.0
  SOVERSION 2)
install(TARGETS radosgw DESTINATION ${CMAKE_INSTALL_LIBDIR})

add_executable(radosgwd radosgw.cc)
target_link_libraries(radosgwd radosgw librados
  cls_rgw_client cls_otp_client cls_lock_client cls_refcount_client
  cls_log_client cls_timeindex_client neorados_cls_fifo
  cls_version_client cls_user_client
  global
  ${LIB_RESOLV}
  ${CURL_LIBRARIES} ${EXPAT_LIBRARIES} ${BLKID_LIBRARIES}
  ${ALLOC_LIBS})
set_target_properties(radosgwd PROPERTIES OUTPUT_NAME radosgw)
install(TARGETS radosgwd DESTINATION bin)

set(radosgw_admin_srcs
  rgw_admin.cc
  rgw_sync_checkpoint.cc
  rgw_orphan.cc)
add_executable(radosgw-admin ${radosgw_admin_srcs})
target_link_libraries(radosgw-admin ${rgw_libs} librados
  cls_rgw_client cls_otp_client cls_lock_client cls_refcount_client
  cls_log_client cls_timeindex_client neorados_cls_fifo
  cls_version_client cls_user_client
  global ${LIB_RESOLV}
  OATH::OATH
  ${CURL_LIBRARIES} ${EXPAT_LIBRARIES} ${BLKID_LIBRARIES})
install(TARGETS radosgw-admin DESTINATION bin)

set(radosgw_es_srcs
  rgw_es_main.cc)
add_executable(radosgw-es ${radosgw_es_srcs})
target_link_libraries(radosgw-es ${rgw_libs} librados
  cls_rgw_client cls_otp_client cls_lock_client cls_refcount_client
  cls_log_client cls_timeindex_client neorados_cls_fifo
  cls_version_client cls_user_client
  global ${LIB_RESOLV}
  ${CURL_LIBRARIES} ${EXPAT_LIBRARIES} ${BLKID_LIBRARIES})
install(TARGETS radosgw-es DESTINATION bin)

set(radosgw_token_srcs
  rgw_token.cc)
add_executable(radosgw-token ${radosgw_token_srcs})
target_link_libraries(radosgw-token librados
  global ${ALLOC_LIBS})
install(TARGETS radosgw-token DESTINATION bin)

set(radosgw_object_expirer_srcs
  rgw_object_expirer.cc)
add_executable(radosgw-object-expirer ${radosgw_object_expirer_srcs})
target_link_libraries(radosgw-object-expirer ${rgw_libs} librados
  cls_rgw_client cls_otp_client cls_lock_client cls_refcount_client
  cls_log_client cls_timeindex_client neorados_cls_fifo
  cls_version_client cls_user_client
  global ${LIB_RESOLV}
  ${CURL_LIBRARIES} ${EXPAT_LIBRARIES})
install(TARGETS radosgw-object-expirer DESTINATION bin)

set(librgw_srcs
  librgw.cc
  rgw_file.cc)
add_library(rgw SHARED ${librgw_srcs})
target_link_libraries(rgw
  PRIVATE
  ${rgw_libs}
  librados
  cls_rgw_client
  cls_otp_client
  cls_lock_client
  cls_refcount_client
  cls_log_client
  cls_timeindex_client
  neorados_cls_fifo
  cls_version_client
  cls_user_client
  global
  ${LIB_RESOLV}
  ${CURL_LIBRARIES}
  ${EXPAT_LIBRARIES}
  PUBLIC
  dmclock::dmclock)

if(WITH_RADOSGW_AMQP_ENDPOINT)
  target_link_libraries(rgw PRIVATE RabbitMQ::RabbitMQ)
  target_link_libraries(rgw PRIVATE OpenSSL::SSL)
endif()

if(WITH_RADOSGW_KAFKA_ENDPOINT)
  target_link_libraries(rgw PRIVATE RDKafka::RDKafka)
endif()

target_link_libraries(rgw PRIVATE ${LUA_LIBRARIES})

set_target_properties(rgw PROPERTIES OUTPUT_NAME rgw VERSION 2.0.0
  SOVERSION 2)
install(TARGETS rgw DESTINATION ${CMAKE_INSTALL_LIBDIR})

if(WITH_TESTS)
  add_executable(ceph_rgw_jsonparser
    rgw_jsonparser.cc)
  target_link_libraries(ceph_rgw_jsonparser
    ${rgw_libs}
    global)

  add_executable(ceph_rgw_multiparser
    rgw_multiparser.cc)
  target_link_libraries(ceph_rgw_multiparser
    ${rgw_libs}
    global)

  install(TARGETS
    ceph_rgw_jsonparser
    ceph_rgw_multiparser
    DESTINATION bin)
endif(WITH_TESTS)

install(PROGRAMS
  rgw-gap-list
  rgw-gap-list-comparator
  rgw-orphan-list
  DESTINATION bin)
