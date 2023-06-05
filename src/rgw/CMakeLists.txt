find_program(GPERF gperf)
if(NOT GPERF)
  message(FATAL_ERROR "Can't find gperf")
endif()

if(WITH_RADOSGW_BACKTRACE_LOGGING)
  add_definitions(-D_BACKTRACE_LOGGING)
endif(WITH_RADOSGW_BACKTRACE_LOGGING)

if(WITH_RADOSGW_SELECT_PARQUET)
  set(ARROW_LIBRARIES Arrow::Arrow Arrow::Parquet)
  add_definitions(-D_ARROW_EXIST)
  message("-- arrow is installed, radosgw/s3select-op is able to process parquet objects")
endif(WITH_RADOSGW_SELECT_PARQUET)

if(WITH_RADOSGW_ARROW_FLIGHT)
  set(ARROW_FLIGHT_LIBRARIES Arrow::Arrow Arrow::Parquet Arrow::Flight utf8proc::utf8proc) # order is important
  add_definitions(-D_ARROW_EXIST)
  message("-- arrow flight is installed")
endif(WITH_RADOSGW_ARROW_FLIGHT)

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
  services/svc_role_rados.cc
  services/svc_sys_obj.cc
  services/svc_sys_obj_cache.cc
  services/svc_sys_obj_core.cc
  services/svc_tier_rados.cc
  services/svc_user.cc
  services/svc_user_rados.cc
  services/svc_zone.cc
  services/svc_zone_utils.cc
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
  rgw_cache.cc
  rgw_common.cc
  rgw_compression.cc
  rgw_cors.cc
  rgw_cors_s3.cc
  rgw_env.cc
  rgw_es_query.cc
  rgw_formats.cc
  rgw_http_client.cc
  rgw_keystone.cc
  rgw_ldap.cc
  rgw_lc.cc
  rgw_lc_s3.cc
  rgw_metadata.cc
  rgw_multi.cc
  rgw_multi_del.cc
  rgw_multipart_meta_filter.cc
  rgw_obj_manifest.cc
  rgw_period.cc
  rgw_realm.cc
  rgw_sync.cc
  rgw_sync_policy.cc
  rgw_notify_event_type.cc
  rgw_period_history.cc
  rgw_period_puller.cc
  rgw_pubsub.cc
  rgw_coroutine.cc
  rgw_cr_rest.cc
  rgw_op.cc
  rgw_policy_s3.cc
  rgw_public_access.cc
  rgw_putobj.cc
  rgw_quota.cc
  rgw_resolve.cc
  rgw_rest.cc
  rgw_rest_client.cc
  rgw_rest_config.cc
  rgw_rest_conn.cc
  rgw_rest_metadata.cc
  rgw_rest_ratelimit.cc
  rgw_rest_role.cc
  rgw_rest_s3.cc
  rgw_rest_pubsub.cc
  rgw_rest_zero.cc
  rgw_s3select.cc
  rgw_role.cc
  rgw_sal.cc
  rgw_sal_filter.cc
  rgw_redis_driver.cc
  rgw_ssd_driver.cc
  rgw_string.cc
  rgw_tag.cc
  rgw_tag_s3.cc
  rgw_tools.cc
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
  rgw_log.cc
  rgw_lua_request.cc
  rgw_lua_utils.cc
  rgw_lua.cc
  rgw_lua_data_filter.cc
  rgw_bucket_encryption.cc
  rgw_tracer.cc
  rgw_lua_background.cc
  rgw_data_access.cc
  driver/rados/cls_fifo_legacy.cc
  driver/rados/rgw_bucket.cc
  driver/rados/rgw_bucket_sync.cc
  driver/rados/rgw_cr_rados.cc
  driver/rados/rgw_cr_tools.cc
  driver/rados/rgw_d3n_datacache.cc
  driver/rados/rgw_datalog.cc
  driver/rados/rgw_datalog_notify.cc
  driver/rados/rgw_data_sync.cc
  driver/rados/rgw_etag_verifier.cc
  driver/rados/rgw_gc.cc
  driver/rados/rgw_gc_log.cc
  driver/rados/rgw_lc_tier.cc
  driver/rados/rgw_log_backing.cc
  driver/rados/rgw_metadata.cc
  driver/rados/rgw_notify.cc
  driver/rados/rgw_obj_manifest.cc
  driver/rados/rgw_object_expirer_core.cc
  driver/rados/rgw_otp.cc
  driver/rados/rgw_period.cc
  driver/rados/rgw_pubsub_push.cc
  driver/rados/rgw_putobj_processor.cc
  driver/rados/rgw_rados.cc
  driver/rados/rgw_reshard.cc
  driver/rados/rgw_rest_bucket.cc
  driver/rados/rgw_rest_log.cc
  driver/rados/rgw_rest_realm.cc
  driver/rados/rgw_rest_user.cc
  driver/rados/rgw_sal_rados.cc
  driver/rados/rgw_service.cc
  driver/rados/rgw_sync.cc
  driver/rados/rgw_sync_counters.cc
  driver/rados/rgw_sync_error_repo.cc
  driver/rados/rgw_sync_module.cc
  driver/rados/rgw_sync_module_aws.cc
  driver/rados/rgw_sync_module_es.cc
  driver/rados/rgw_sync_module_es_rest.cc
  driver/rados/rgw_sync_module_log.cc
  driver/rados/rgw_sync_trace.cc
  driver/rados/rgw_tools.cc
  driver/rados/rgw_trim_bilog.cc
  driver/rados/rgw_trim_datalog.cc
  driver/rados/rgw_trim_mdlog.cc
  driver/rados/rgw_user.cc
  driver/rados/rgw_zone.cc
  driver/rados/sync_fairness.cc
  driver/rados/topic.cc)

list(APPEND librgw_common_srcs
  driver/immutable_config/store.cc
  driver/json_config/store.cc
  driver/rados/config/impl.cc
  driver/rados/config/period.cc
  driver/rados/config/period_config.cc
  driver/rados/config/realm.cc
  driver/rados/config/store.cc
  driver/rados/config/zone.cc
  driver/rados/config/zonegroup.cc)

if(WITH_RADOSGW_AMQP_ENDPOINT)
  list(APPEND librgw_common_srcs rgw_amqp.cc)
endif()
if(WITH_RADOSGW_KAFKA_ENDPOINT)
  list(APPEND librgw_common_srcs rgw_kafka.cc)
endif()
if(WITH_RADOSGW_DBSTORE)
  add_subdirectory(driver/dbstore)
  list(APPEND librgw_common_srcs rgw_sal_dbstore.cc)
endif()
if(WITH_RADOSGW_MOTR)
  list(APPEND librgw_common_srcs driver/motr/rgw_sal_motr.cc)
endif()
if(WITH_RADOSGW_DAOS)
  list(APPEND librgw_common_srcs driver/motr/rgw_sal_daos.cc)
endif()
if(WITH_RADOSGW_D4N)
  list(APPEND librgw_common_srcs driver/d4n/d4n_directory.cc)
  list(APPEND librgw_common_srcs driver/d4n/d4n_datacache.cc)
  list(APPEND librgw_common_srcs driver/d4n/rgw_sal_d4n.cc)
endif()
if(WITH_RADOSGW_POSIX)
  #add_subdirectory(driver/posix)
  find_package(LMDB REQUIRED)
  add_compile_definitions(LMDB_SAFE_NO_CPP_UTILITIES)
  list(APPEND librgw_common_srcs
	      driver/posix/rgw_sal_posix.cc
	      driver/posix/lmdb-safe.cc
	      driver/posix/notify.cpp)
endif()
if(WITH_JAEGER)
  list(APPEND librgw_common_srcs rgw_tracer.cc)
endif()
if(WITH_RADOSGW_ARROW_FLIGHT)
  # NOTE: eventually don't want this in common but just in radosgw daemon
  # list(APPEND radosgw_srcs rgw_flight.cc rgw_flight_frontend.cc)
  list(APPEND librgw_common_srcs rgw_flight.cc rgw_flight_frontend.cc)
endif(WITH_RADOSGW_ARROW_FLIGHT)

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
    global
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
    libneorados
    rt
    ICU::uc
    OATH::OATH
    dmclock::dmclock
    ${CURL_LIBRARIES}
    ${EXPAT_LIBRARIES}
    ${ARROW_LIBRARIES}
    ${ARROW_FLIGHT_LIBRARIES}
    ${LMDB_LIBRARIES}
  PUBLIC
    ${LUA_LIBRARIES}
    RapidJSON::RapidJSON
    spawn
    ${FMT_LIB})
target_include_directories(rgw_common
  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw/services"
  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
  PUBLIC "${LUA_INCLUDE_DIR}")

if(WITH_RADOSGW_D4N)
  add_dependencies(rgw_common cpp_redis)
  target_link_libraries(rgw_common PRIVATE cpp_redis)
  target_include_directories(rgw_common SYSTEM PUBLIC "${CMAKE_SOURCE_DIR}/src/cpp_redis/includes")
  target_include_directories(rgw_common SYSTEM PUBLIC "${CMAKE_SOURCE_DIR}/src/cpp_redis/tacopie/includes")
endif()

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
  add_dependencies(rgw_common jaeger_base)
  target_link_libraries(rgw_common PUBLIC jaeger_base)
endif()

if(WITH_RADOSGW_DBSTORE)
  target_link_libraries(rgw_common PRIVATE global dbstore)
endif()

if(WITH_RADOSGW_MOTR)
  find_package(motr REQUIRED)
  target_link_libraries(rgw_common PRIVATE motr::motr)
endif()

if(WITH_RADOSGW_DAOS)
  find_package(DAOS REQUIRED)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG ")
  target_link_libraries(rgw_common PRIVATE daos dfs ds3 uuid duns)
  target_include_directories(rgw_common PRIVATE ${PC_DAOS_INCLUDEDIR} )
  link_directories( ${PC_DAOS_LIBRARY_DIRS} )
endif()

set(rgw_a_srcs
  rgw_appmain.cc
  rgw_asio_client.cc
  rgw_asio_frontend.cc
  rgw_auth_keystone.cc
  rgw_client_io.cc
  rgw_file.cc
  rgw_frontend.cc
  rgw_http_client_curl.cc
  rgw_kmip_client_impl.cc
  rgw_lib.cc
  rgw_loadgen.cc
  rgw_loadgen_process.cc
  rgw_log.cc
  rgw_lua_request.cc
  rgw_opa.cc
  rgw_os_lib.cc
  rgw_period_pusher.cc
  rgw_process.cc
  rgw_realm_reloader.cc
  rgw_realm_watcher.cc
  rgw_rest_config.cc
  rgw_rest_info.cc
  rgw_rest_metadata.cc
  rgw_rest_ratelimit.cc
  rgw_rest_sts.cc
  rgw_rest_swift.cc
  rgw_rest_usage.cc
  rgw_signal.cc
  rgw_swift_auth.cc
  rgw_usage.cc
  rgw_sts.cc
  driver/rados/rgw_rest_bucket.cc
  driver/rados/rgw_rest_log.cc
  driver/rados/rgw_rest_realm.cc)

gperf_generate(${CMAKE_SOURCE_DIR}/src/rgw/rgw_iam_policy_keywords.gperf
  rgw_iam_policy_keywords.frag.cc)
set_source_files_properties(rgw_iam_policy.cc PROPERTIES
  OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/src/rgw/rgw_iam_policy_keywords.frag.cc
  COMPILE_FLAGS -I${CMAKE_BINARY_DIR}/src/rgw)


add_library(rgw_a STATIC
    ${rgw_a_srcs})

target_compile_definitions(rgw_a PUBLIC "-DCLS_CLIENT_HIDE_IOCTX")

target_include_directories(rgw_a
  PUBLIC "${CMAKE_SOURCE_DIR}/src/dmclock/support/src"
  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw/driver/rados"
  PRIVATE "${CMAKE_SOURCE_DIR}/src/libkmip")

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
    ${ARROW_LIBRARIES}
    ${ARROW_FLIGHT_LIBRARIES}
    OATH::OATH
  PUBLIC
    rgw_common
    spawn)

if(WITH_CURL_OPENSSL)
  # used by rgw_http_client_curl.cc
  target_link_libraries(rgw_a PRIVATE OpenSSL::Crypto)
endif()

set(rgw_libs rgw_a)

set(rgw_schedulers_srcs
  rgw_dmclock_scheduler_ctx.cc
  rgw_dmclock_sync_scheduler.cc
  rgw_dmclock_async_scheduler.cc)

add_library(rgw_schedulers STATIC ${rgw_schedulers_srcs})
target_link_libraries(rgw_schedulers
  PUBLIC dmclock::dmclock spawn)

set(radosgw_srcs
  rgw_main.cc)

add_executable(radosgw ${radosgw_srcs})

if(WITH_RADOSGW_ARROW_FLIGHT)
  # target_compile_definitions(radosgw PUBLIC WITH_ARROW_FLIGHT)
  target_compile_definitions(rgw_common PUBLIC WITH_ARROW_FLIGHT)
  target_include_directories(rgw_common
    PUBLIC "${CMAKE_SOURCE_DIR}/src/arrow/cpp/src")
  # target_include_directories(radosgw PUBLIC Arrow::Arrow)
endif(WITH_RADOSGW_ARROW_FLIGHT)

target_compile_definitions(radosgw PUBLIC "-DCLS_CLIENT_HIDE_IOCTX")
target_include_directories(radosgw
  PUBLIC "${CMAKE_SOURCE_DIR}/src/dmclock/support/src"
  PRIVATE "${CMAKE_SOURCE_DIR}/src/libkmip"
  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
  PRIVATE "${LUA_INCLUDE_DIR}")

target_include_directories(radosgw SYSTEM PUBLIC "../rapidjson/include")

target_link_libraries(radosgw PRIVATE
  ${rgw_libs}
  rgw_schedulers
  kmip
  ${ALLOC_LIBS})

if(WITH_RADOSGW_BEAST_OPENSSL)
  # used by rgw_asio_frontend.cc
  target_link_libraries(radosgw PRIVATE OpenSSL::SSL)
endif()
install(TARGETS radosgw DESTINATION bin)

set(radosgw_admin_srcs
  rgw_admin.cc
  rgw_sync_checkpoint.cc
  rgw_orphan.cc)

# this is unsatisfying and hopefully temporary; ARROW should not be
# part of radosgw_admin
if(WITH_RADOSGW_ARROW_FLIGHT)
  list(APPEND radosgw_admin_srcs rgw_flight.cc)
endif(WITH_RADOSGW_ARROW_FLIGHT)

add_executable(radosgw-admin ${radosgw_admin_srcs})
target_link_libraries(radosgw-admin ${rgw_libs} librados
  cls_rgw_client cls_otp_client cls_lock_client cls_refcount_client
  cls_log_client cls_timeindex_client
  cls_version_client cls_user_client
  global ${LIB_RESOLV}
  OATH::OATH
  ${CURL_LIBRARIES} ${EXPAT_LIBRARIES} ${BLKID_LIBRARIES})

# this is unsatisfying and hopefully temporary; ARROW should not be
# part of radosgw_admin
if(WITH_RADOSGW_ARROW_FLIGHT)
  target_link_libraries(radosgw-admin ${ARROW_LIBRARIES} ${ARROW_FLIGHT_LIBRARIES})
endif(WITH_RADOSGW_ARROW_FLIGHT)

install(TARGETS radosgw-admin DESTINATION bin)

set(radosgw_es_srcs
  rgw_es_main.cc)
add_executable(radosgw-es ${radosgw_es_srcs})
target_link_libraries(radosgw-es ${rgw_libs} librados
  cls_rgw_client cls_otp_client cls_lock_client cls_refcount_client
  cls_log_client cls_timeindex_client
  cls_version_client cls_user_client
  global ${LIB_RESOLV}
  ${CURL_LIBRARIES} ${EXPAT_LIBRARIES} ${BLKID_LIBRARIES})
install(TARGETS radosgw-es DESTINATION bin)

set(radosgw_token_srcs
  rgw_token.cc)
add_executable(radosgw-token ${radosgw_token_srcs})
target_link_libraries(radosgw-token librados
  global)
install(TARGETS radosgw-token DESTINATION bin)

set(radosgw_object_expirer_srcs
  rgw_object_expirer.cc)
add_executable(radosgw-object-expirer ${radosgw_object_expirer_srcs})
target_link_libraries(radosgw-object-expirer ${rgw_libs} librados
  cls_rgw_client cls_otp_client cls_lock_client cls_refcount_client
  cls_log_client cls_timeindex_client
  cls_version_client cls_user_client
  global ${LIB_RESOLV}
  ${CURL_LIBRARIES} ${EXPAT_LIBRARIES})
install(TARGETS radosgw-object-expirer DESTINATION bin)

set(radosgw_polparser_srcs
  rgw_polparser.cc)
add_executable(rgw-policy-check ${radosgw_polparser_srcs})
target_link_libraries(rgw-policy-check ${rgw_libs})
install(TARGETS rgw-policy-check DESTINATION bin)

set(librgw_srcs
  librgw.cc)
add_library(rgw SHARED ${librgw_srcs})

target_compile_definitions(rgw PUBLIC "-DCLS_CLIENT_HIDE_IOCTX")
target_include_directories(rgw
  PUBLIC "${CMAKE_SOURCE_DIR}/src/dmclock/support/src"
  PRIVATE "${CMAKE_SOURCE_DIR}/src/libkmip"
  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
  PRIVATE "${LUA_INCLUDE_DIR}")

target_include_directories(rgw SYSTEM PUBLIC "../rapidjson/include")

target_link_libraries(rgw
  PRIVATE
  ${rgw_libs}
  rgw_schedulers
  kmip
  librados
  cls_rgw_client
  cls_otp_client
  cls_lock_client
  cls_refcount_client
  cls_log_client
  cls_timeindex_client
  cls_version_client
  cls_user_client
  ${LIB_RESOLV}
  ${CURL_LIBRARIES}
  ${EXPAT_LIBRARIES}
  OpenSSL::SSL
  PUBLIC
  RapidJSON::RapidJSON
  dmclock::dmclock)

if(WITH_RADOSGW_AMQP_ENDPOINT)
  target_link_libraries(rgw PRIVATE RabbitMQ::RabbitMQ)
endif()

if(WITH_RADOSGW_KAFKA_ENDPOINT)
  target_link_libraries(rgw PRIVATE RDKafka::RDKafka)
endif()

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
  rgw-restore-bucket-index
  DESTINATION bin)
