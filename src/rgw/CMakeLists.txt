find_program(GPERF gperf
  REQUIRED)

option(USE_SQLITE "Enable SQLITE DB" ON)
if(USE_SQLITE)
  find_package(SQLite3 REQUIRED)
  add_compile_definitions(SQLITE_ENABLED=1)
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

if(CEPH_HAVE_ISAL)
  message("-- building RGW with ISA-L support")
  add_definitions(-DSPDK_CONFIG_ISAL)
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
  spdk/crc64.c
  madler/crc64nvme.c
  madler/crc32iso_hdlc.c
  madler/crc32iscsi.c
  rgw_account.cc
  rgw_acl.cc
  rgw_acl_s3.cc
  rgw_acl_swift.cc
  rgw_aio.cc
  rgw_aio_throttle.cc
  rgw_asio_thread.cc
  rgw_auth.cc
  rgw_auth_s3.cc
  rgw_arn.cc
  rgw_basic_types.cc
  rgw_bucket.cc
  rgw_bucket_layout.cc
  rgw_cache.cc
  rgw_cksum.cc
  rgw_cksum_pipe.cc
  rgw_common.cc
  rgw_compression.cc
  rgw_cors.cc
  rgw_cors_s3.cc
  rgw_env.cc
  rgw_es_query.cc
  rgw_formats.cc
  rgw_http_client.cc
  rgw_keystone.cc
  rgw_keystone_scope.cc
  rgw_ldap.cc
  rgw_lc.cc
  rgw_lc_s3.cc
  rgw_restore.cc
  rgw_restore_waiter.cc
  rgw_metadata.cc
  rgw_multi.cc
  rgw_multi_del.cc
  rgw_multipart_meta_filter.cc
  rgw_obj_manifest.cc
  rgw_object_ownership.cc
  rgw_period.cc
  rgw_realm.cc
  rgw_sync_policy.cc
  rgw_notify_event_type.cc
  rgw_period_history.cc
  rgw_period_puller.cc
  rgw_s3_filter.cc
  rgw_pubsub.cc
  rgw_cr_rest.cc
  rgw_op.cc
  rgw_policy_s3.cc
  rgw_public_access.cc
  rgw_putobj.cc
  rgw_quota.cc
  rgw_resolve.cc
  rgw_rest.cc
  rgw_rest_account.cc
  rgw_rest_client.cc
  rgw_rest_config.cc
  rgw_rest_conn.cc
  rgw_rest_dedup.cc
  rgw_rest_metadata.cc
  rgw_rest_ratelimit.cc
  rgw_rest_role.cc
  rgw_rest_iam_account.cc
  rgw_rest_iam_group.cc
  rgw_rest_iam_user.cc
  rgw_rest_s3.cc
  rgw_rest_s3control.cc
  rgw_rest_pubsub.cc
  rgw_rest_zero.cc
  rgw_s3select.cc
  rgw_role.cc
  rgw_sal.cc
  rgw_sal_filter.cc
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
  rgw_iam_managed_policy.cc
  rgw_rest_user_policy.cc
  rgw_zone.cc
  rgw_sts.cc
  rgw_rest_sts.cc
  rgw_perf_counters.cc
  rgw_rest_oidc_provider.cc
  rgw_rest_iam.cc
  rgw_object_lock.cc
  rgw_kms.cc
  rgw_kms_cache.cc
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
  rgw_realm_watcher.cc
  rgw_bucket_logging.cc
  rgw_rest_bucket_logging.cc
  rgw_bucket_sync.cc
  rgw_rest_restore.cc)

list(APPEND librgw_common_srcs
  driver/immutable_config/store.cc
  driver/json_config/store.cc)

if(WITH_RADOSGW_AMQP_ENDPOINT)
  list(APPEND librgw_common_srcs rgw_amqp.cc)
endif()
if(WITH_RADOSGW_KAFKA_ENDPOINT)
  list(APPEND librgw_common_srcs rgw_kafka.cc)
endif()
if(WITH_JAEGER)
  list(APPEND librgw_common_srcs rgw_tracer.cc)
endif()

if(WITH_RADOSGW_ARROW_FLIGHT)
  # NOTE: eventually don't want this in common but just in radosgw daemon
  # list(APPEND radosgw_srcs rgw_flight.cc rgw_flight_frontend.cc)
  list(APPEND librgw_common_srcs rgw_flight.cc rgw_flight_frontend.cc)
endif(WITH_RADOSGW_ARROW_FLIGHT)

list(APPEND librgw_rados_srcs
  services/svc_bi_rados.cc
  services/svc_bilog_rados.cc
  services/svc_bucket.cc
  services/svc_bucket_sobj.cc
  services/svc_bucket_sync_sobj.cc
  services/svc_cls.cc
  services/svc_config_key_rados.cc
  services/svc_mdlog.cc
  services/svc_notify.cc
  services/svc_quota.cc
  services/svc_sync_modules.cc
  services/svc_sys_obj.cc
  services/svc_sys_obj_cache.cc
  services/svc_sys_obj_core.cc
  services/svc_user.cc
  services/svc_user_rados.cc
  services/svc_zone.cc
  services/svc_zone_utils.cc
  driver/rados/account.cc
  driver/rados/buckets.cc
  driver/rados/group.cc
  driver/rados/groups.cc
  driver/rados/rgw_bucket.cc
  driver/rados/rgw_bl_rados.cc
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
  driver/rados/oidc.cc
  driver/rados/oidcs.cc
  driver/rados/role.cc
  driver/rados/roles.cc
  driver/rados/sync_fairness.cc
  driver/rados/topic.cc
  driver/rados/topic_migration.cc
  driver/rados/topics.cc
  driver/rados/users.cc
  driver/rados/config/impl.cc
  driver/rados/config/period.cc
  driver/rados/config/period_config.cc
  driver/rados/config/realm.cc
  driver/rados/config/realm_watcher.cc
  driver/rados/config/store.cc
  driver/rados/config/zone.cc
  driver/rados/config/zonegroup.cc
  driver/rados/rgw_dedup.cc
  driver/rados/rgw_dedup_filter.cc
  driver/rados/rgw_dedup_table.cc
  driver/rados/rgw_dedup_store.cc
  driver/rados/rgw_dedup_utils.cc
  driver/rados/rgw_dedup_cluster.cc
  rgw_coroutine.cc
)
list(APPEND librgw_dbstore_srcs
  rgw_sal_dbstore.cc
  driver/dbstore/common/dbstore_log.h
  driver/dbstore/common/dbstore.h
  driver/dbstore/common/dbstore.cc
  driver/dbstore/config/store.cc
  driver/dbstore/config/sqlite.cc
  driver/dbstore/sqlite/connection.cc
  driver/dbstore/sqlite/error.cc
  driver/dbstore/sqlite/statement.cc
  driver/dbstore/dbstore_mgr.h
  driver/dbstore/dbstore_mgr.cc
  driver/dbstore/sqlite/sqliteDB.h
  driver/dbstore/sqlite/sqliteDB.cc
)
list(APPEND librgw_motr_srcs
  driver/motr/rgw_sal_motr.cc
)
list(APPEND librgw_daos_srcs
  driver/motr/rgw_sal_daos.cc
)
list(APPEND librgw_posix_srcs
  driver/posix/rgw_sal_posix.cc
  driver/posix/lmdb-safe.cc
  driver/posix/posixDB.cc
  driver/posix/notify.cpp
)
list(APPEND librgw_d4n_srcs
  rgw_redis_driver.cc
  rgw_ssd_driver.cc
  driver/d4n/d4n_directory.cc
  driver/d4n/d4n_policy.cc
  driver/d4n/rgw_sal_d4n.cc)

set(rgw_common_libs)

if(WITH_RADOSGW_RADOS)
  list(APPEND librgw_driver_srcs ${librgw_rados_srcs})
endif()
if(WITH_RADOSGW_DBSTORE OR WITH_RADOSGW_STANDALONE)
  list(APPEND librgw_driver_srcs ${librgw_dbstore_srcs})
  list(APPEND librgw_driver_standalone_srcs ${librgw_dbstore_srcs})
  list(APPEND rgw_common_libs sqlite3)
set(SQLITE_COMPILE_FLAGS "-DSQLITE_THREADSAFE=1")
endif()
if(WITH_RADOSGW_MOTR)
  list(APPEND librgw_driver_srcs ${librgw_motr_srcs})
endif()
if(WITH_RADOSGW_DAOS)
  list(APPEND librgw_driver_srcs ${librgw_daos_srcs})
endif()
if(WITH_RADOSGW_POSIX)
  find_package(LMDB REQUIRED)
  add_compile_definitions(LMDB_SAFE_NO_CPP_UTILITIES)
  list(APPEND librgw_driver_srcs ${librgw_posix_srcs})
  list(APPEND librgw_driver_standalone_srcs ${librgw_posix_srcs})
endif()
if(WITH_RADOSGW_D4N)
  list(APPEND librgw_driver_srcs ${librgw_d4n_srcs})
endif()

if(WITH_RADOSGW_STANDALONE)
  find_package(LMDB REQUIRED)
  add_compile_definitions(LMDB_SAFE_NO_CPP_UTILITIES)
  list(APPEND librgw_driver_standalone_srcs
    ${librgw_posix_srcs}
    driver/posix/rgw_standalone.cc
  )
endif()

add_library(rgw_common STATIC
  ${librgw_common_srcs}
  ${librgw_driver_srcs})

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-Wimplicit-const-int-float-conversion"
  COMPILER_SUPPORTS_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION)
if(COMPILER_SUPPORTS_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION)
  target_compile_definitions(common-objs PRIVATE
    HAVE_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION)
endif()

if(COMPILER_SUPPORTS_VLA_ERROR)
  # Will be picked up by anyone including rgw_common
  target_compile_options(rgw_common PUBLIC
    $<$<COMPILE_LANGUAGE:CXX>:-Werror=vla>)
endif()

target_link_libraries(rgw_common
  PRIVATE
    ${rgw_common_libs}
    legacy-option-headers
    global
    cls_rgw_client
    keyring
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
    Boost::context
    Boost::url
    ${FMT_LIB}
    OpenSSL::SSL
    BLAKE3::blake3)
target_include_directories(rgw_common
  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw/services"
  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
  PUBLIC "${LUA_INCLUDE_DIR}")

# work around https://github.com/Cyan4973/xxHash/issues/943 for debug builds
target_compile_definitions(rgw_common PUBLIC
  $<$<CONFIG:Debug>:XXH_NO_INLINE_HINTS=1>
  $<$<CONFIG:RelWithDebInfo>:XXH_NO_INLINE_HINTS=1>)

#target_compile_definitions(rgw_common
  #PUBLIC "-DCLS_CLIENT_HIDE_IOCTX")

if(WITH_RADOSGW_RADOS)
  target_compile_OPTIONS(rgw_common
    PUBLIC "-DWITH_RADOSGW_RADOS")
endif()

if(WITH_RADOSGW_DBSTORE OR WITH_RADOSGW_STANDALONE)
  target_compile_OPTIONS(rgw_common PUBLIC "-DSQLITE_THREADSAFE=1")
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
      RabbitMQ::RabbitMQ)
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

if(WITH_RADOSGW_RADOS)
  target_link_libraries(rgw_common PRIVATE
	  cls_2pc_queue_client
	  cls_cmpomap_client
	  cls_lock_client
	  cls_log_client
	  cls_otp_client
	  cls_refcount_client
	  cls_rgw_gc_client
	  cls_timeindex_client
	  cls_user_client
	  cls_version_client
	  librados
	  libneorados
  )
endif()

if(WITH_RADOSGW_MOTR)
  find_package(motr REQUIRED)
  list(APPEND rgw_libs motr::motr)
endif()

if(WITH_RADOSGW_DAOS)
  find_package(DAOS REQUIRED)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG ")
  list(APPEND rgw_libs daos dfs ds3 uuid duns)
  target_include_directories(rgw_common PRIVATE ${PC_DAOS_INCLUDEDIR} )
  link_directories( ${PC_DAOS_LIBRARY_DIRS} )
endif()

if(WITH_RADOSGW_STANDALONE)
  add_library(rgw_common_standalone STATIC
    ${librgw_common_srcs}
    ${librgw_driver_standalone_srcs})

  include(CheckCXXCompilerFlag)
  check_cxx_compiler_flag("-Wimplicit-const-int-float-conversion"
    COMPILER_SUPPORTS_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION)
  if(COMPILER_SUPPORTS_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION)
    target_compile_definitions(common-objs PRIVATE
      HAVE_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION)
  endif()

  target_link_libraries(rgw_common_standalone
    PRIVATE
    ${rgw_common_libs}
    legacy-option-headers
    global
    cls_rgw_client
    keyring
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
    Boost::context
    Boost::url
    ${FMT_LIB}
    OpenSSL::SSL
    BLAKE3::blake3)
  target_include_directories(rgw_common_standalone
    PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw/services"
    PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
    PUBLIC "${LUA_INCLUDE_DIR}")

  # work around https://github.com/Cyan4973/xxHash/issues/943 for debug builds
  target_compile_definitions(rgw_common_standalone PUBLIC
    $<$<CONFIG:Debug>:XXH_NO_INLINE_HINTS=1>
    $<$<CONFIG:RelWithDebInfo>:XXH_NO_INLINE_HINTS=1>)

  target_compile_OPTIONS(rgw_common_standalone
    PUBLIC "-DWITH_RADOSGW_STANDALONE"
    PUBLIC "-DWITH_RADOSGW_DBSTORE"
    PUBLIC "-UWITH_RADOSGW_D4N"
    PUBLIC "-UWITH_RADOSGW_RADOS"
    PUBLIC "-UWITH_RADOSGW_MOTR"
    PUBLIC "-UWITH_RADOSGW_DAOS"
    PUBLIC "-UWITH_RADOSGW_AMQP_ENDPOINT")

  if(WITH_RADOSGW_KAFKA_ENDPOINT)
    # used by rgw_kafka.cc
    target_link_libraries(rgw_common_standalone
      PRIVATE
      RDKafka::RDKafka)
  endif()
  if(WITH_OPENLDAP)
    target_link_libraries(rgw_common_standalone
      PRIVATE
      OpenLDAP::OpenLDAP)
  endif()
  if(WITH_RADOSGW_LUA_PACKAGES)
    target_link_libraries(rgw_common_standalone
      PRIVATE Boost::filesystem StdFilesystem::filesystem)
  endif()

  if(WITH_JAEGER)
    add_dependencies(rgw_common_standalone jaeger_base)
    target_link_libraries(rgw_common_standalone PUBLIC jaeger_base)
  endif()
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
  rgw_process.cc
  rgw_realm_reloader.cc
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
  rgw_sts.cc)

set(rgw_a_rados_srcs "")

if(WITH_RADOSGW_RADOS)
  list(APPEND rgw_a_rados_srcs
    rgw_period_pusher.cc
    rgw_rest_dedup.cc
  )
endif()

gperf_generate(${CMAKE_SOURCE_DIR}/src/rgw/rgw_iam_policy_keywords.gperf
  rgw_iam_policy_keywords.frag.cc)
set_source_files_properties(rgw_iam_policy.cc PROPERTIES
  OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/src/rgw/rgw_iam_policy_keywords.frag.cc
  COMPILE_FLAGS -I${CMAKE_BINARY_DIR}/src/rgw)


add_library(rgw_a STATIC
    ${rgw_a_srcs}
    ${rgw_a_rados_srcs})

target_compile_definitions(rgw_a PUBLIC "-DCLS_CLIENT_HIDE_IOCTX")

if(WITH_RADOSGW_RADOS)
  target_compile_OPTIONS(rgw_a
    PUBLIC "-DWITH_RADOSGW_RADOS")
endif()

target_include_directories(rgw_a
  PUBLIC "${CMAKE_SOURCE_DIR}/src/dmclock/support/src"
  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw/driver/rados"
  PRIVATE "${CMAKE_SOURCE_DIR}/src/libkmip")

if(WITH_RADOSGW_AMQP_ENDPOINT)
  find_package(RabbitMQ REQUIRED)
endif()
if(WITH_RADOSGW_KAFKA_ENDPOINT)
  set(rdkafka_min_version 2.11)
  if(WITH_SYSTEM_RDKAFKA)
    find_package(RDKafka ${rdkafka_min_version} REQUIRED)
  elseif(EXISTS "${CMAKE_SOURCE_DIR}/src/librdkafka/CMakeLists.txt")
    message(STATUS "Building librdkafka as submodule")
    set(WITH_SSL ON)
    set(WITH_SASL ON)
    set(RDKAFKA_BUILD_EXAMPLES OFF)
    set(RDKAFKA_BUILD_TESTS OFF)
    set(RDKAFKA_BUILD_STATIC ON)
    add_subdirectory(${CMAKE_SOURCE_DIR}/src/librdkafka
                     ${CMAKE_BINARY_DIR}/src/librdkafka
                     EXCLUDE_FROM_ALL)
    target_compile_options(rdkafka PRIVATE -w)
    if(NOT TARGET RDKafka::RDKafka)
      add_library(RDKafka::RDKafka ALIAS rdkafka)
    endif()
    # create a compatibility symlink, so that rgw_a can include <librdkafka/rdkafka.h>
    # both for the submodule and system librdkafka
    set(rdkafka_compat_dir "${CMAKE_BINARY_DIR}/src/librdkafka-compat")
    file(MAKE_DIRECTORY "${rdkafka_compat_dir}")
    execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink
      "${CMAKE_SOURCE_DIR}/src/librdkafka/src"
      "${rdkafka_compat_dir}/librdkafka")
    target_include_directories(rdkafka PUBLIC "$<BUILD_INTERFACE:${rdkafka_compat_dir}>")
  else()
    message(STATUS "src/librdkafka submodule not present; falling back to system librdkafka")
    find_package(RDKafka ${rdkafka_min_version} REQUIRED)
  endif()
endif()

target_link_libraries(rgw_a
  PRIVATE
    rgw_schedulers
    kmip
    legacy-option-headers
    common_utf8 global
    ${CRYPTO_LIBS}
    ${ARROW_LIBRARIES}
    ${ARROW_FLIGHT_LIBRARIES}
    OATH::OATH
  PUBLIC
    rgw_common
    Boost::context)

if(WITH_CURL_OPENSSL)
  # used by rgw_http_client_curl.cc
  target_link_libraries(rgw_a PRIVATE OpenSSL::Crypto)
endif()

list(APPEND rgw_libs rgw_a)

if(WITH_RADOSGW_STANDALONE)
  add_library(rgw_a_standalone STATIC
    ${rgw_a_srcs})

  target_compile_OPTIONS(rgw_a_standalone
    PUBLIC "-DWITH_RADOSGW_STANDALONE"
    PUBLIC "-DWITH_RADOSGW_DBSTORE"
    PUBLIC "-UWITH_RADOSGW_D4N"
    PUBLIC "-UWITH_RADOSGW_RADOS"
    PUBLIC "-UWITH_RADOSGW_MOTR"
    PUBLIC "-UWITH_RADOSGW_DAOS")

  target_include_directories(rgw_a_standalone
    PUBLIC "${CMAKE_SOURCE_DIR}/src/dmclock/support/src"
    PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
    PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw/driver/rados"
    PRIVATE "${CMAKE_SOURCE_DIR}/src/libkmip")

  target_link_libraries(rgw_a_standalone
    PRIVATE
    rgw_schedulers
    kmip
    legacy-option-headers
    common_utf8 global
    ${CRYPTO_LIBS}
    ${ARROW_LIBRARIES}
    ${ARROW_FLIGHT_LIBRARIES}
    OATH::OATH
    PUBLIC
    rgw_common_standalone
    Boost::context)

  if(WITH_CURL_OPENSSL)
    # used by rgw_http_client_curl.cc
    target_link_libraries(rgw_a_standalone PRIVATE OpenSSL::Crypto)
  endif()
endif()

set(rgw_schedulers_srcs
  rgw_dmclock_scheduler_ctx.cc
  rgw_dmclock_sync_scheduler.cc
  rgw_dmclock_async_scheduler.cc)

add_library(rgw_schedulers STATIC ${rgw_schedulers_srcs})
target_link_libraries(rgw_schedulers
  PUBLIC dmclock::dmclock Boost::context)
if(COMPILER_SUPPORTS_VLA_ERROR)
  target_compile_options(rgw_schedulers PRIVATE
    $<$<COMPILE_LANGUAGE:CXX>:-Werror=vla>)
endif()

set(radosgw_srcs
  rgw_main.cc)

add_executable(radosgw ${radosgw_srcs})

if(WITH_RADOSGW_RADOS)
  target_compile_OPTIONS(radosgw
    PUBLIC "-DWITH_RADOSGW_RADOS")
endif()

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
  PUBLIC "${CMAKE_SOURCE_DIR}/src/s3select/rapidjson/include"
  PRIVATE "${LUA_INCLUDE_DIR}")

target_link_libraries(radosgw PRIVATE
  legacy-option-headers
  ${rgw_libs}
  ${ALLOC_LIBS})

if(WITH_RADOSGW_FDB)
 message("FoundationDB support is active (experimental)")

 add_library(rgw_fdb INTERFACE)

 target_link_libraries(rgw_fdb INTERFACE
  -lfdb_c
  -lm
  -lrt
 )

 target_compile_options(rgw_fdb INTERFACE
   -pthread)

#JFW: this is where the FDB client library packages install
#fdb_c headers-- we need to find a smarter way to handle this
#end of it before libfdb can be non-experimental:
 target_include_directories(rgw_fdb INTERFACE
  /usr/include/foundationdb)

 target_include_directories(rgw_common PRIVATE
  /usr/include/foundationdb
  "${CMAKE_SOURCE_DIR}/src/rgw")

 target_link_libraries(rgw_common PRIVATE
  rgw_fdb)

 target_include_directories(radosgw PRIVATE
  /usr/include/foundationdb)

 # Use the zpp_bits back-end for serialization:
 add_library(zpp_bits INTERFACE)

 # JFW: there must be a nicer way to refer to this path (or ask it to be installed somewhere?):
 target_include_directories(zpp_bits INTERFACE "${CMAKE_BINARY_DIR}/_deps/zpp_bits-src/")

 target_link_libraries(rgw_fdb INTERFACE zpp_bits)

endif(WITH_RADOSGW_FDB)

install(TARGETS radosgw DESTINATION bin)

if(WITH_RADOSGW_STANDALONE)
  add_executable(rgw-standalone ${radosgw_srcs})

  target_compile_OPTIONS(rgw-standalone
    PUBLIC "-DWITH_RADOSGW_STANDALONE"
    PUBLIC "-DWITH_RADOSGW_DBSTORE"
    PUBLIC "-UWITH_RADOSGW_D4N"
    PUBLIC "-UWITH_RADOSGW_RADOS"
    PUBLIC "-UWITH_RADOSGW_MOTR"
    PUBLIC "-UWITH_RADOSGW_DAOS")

  target_include_directories(rgw-standalone
    PUBLIC "${CMAKE_SOURCE_DIR}/src/dmclock/support/src"
    PRIVATE "${CMAKE_SOURCE_DIR}/src/libkmip"
    PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
    PUBLIC "${CMAKE_SOURCE_DIR}/src/s3select/rapidjson/include"
    PRIVATE "${LUA_INCLUDE_DIR}")

  target_link_libraries(rgw-standalone PRIVATE
    legacy-option-headers
    rgw_a_standalone
    ${ALLOC_LIBS})

  install(TARGETS rgw-standalone DESTINATION bin)
endif()

set(radosgw_admin_srcs
  radosgw-admin/radosgw-admin.cc)

# this is unsatisfying and hopefully temporary; ARROW should not be
# part of radosgw_admin
if(WITH_RADOSGW_ARROW_FLIGHT)
  list(APPEND radosgw_admin_srcs rgw_flight.cc)
endif(WITH_RADOSGW_ARROW_FLIGHT)

if(WITH_RADOSGW_RADOS)
  list(APPEND radosgw_admin_rados_srcs
    radosgw-admin/sync_checkpoint.cc
    radosgw-admin/orphan.cc)

  add_executable(radosgw-admin ${radosgw_admin_srcs} ${radosgw_admin_rados_srcs})
  target_link_libraries(radosgw-admin
    legacy-option-headers
    ${rgw_libs} librados
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

  target_compile_OPTIONS(radosgw-admin PUBLIC "-DWITH_RADOSGW_RADOS")

  install(TARGETS radosgw-admin DESTINATION bin)
endif()

if(WITH_RADOSGW_STANDALONE)
  list(APPEND rgw_standalone_admin_srcs
    driver/rados/rgw_user.cc
    driver/rados/rgw_bucket.cc
    driver/rados/rgw_obj_manifest.cc)

  add_executable(rgw-standalone-admin ${radosgw_admin_srcs} ${rgw_standalone_admin_srcs})

  target_link_libraries(rgw-standalone-admin
    legacy-option-headers
    rgw_a_standalone
    global ${LIB_RESOLV}
    OATH::OATH
    ${CURL_LIBRARIES} ${EXPAT_LIBRARIES} ${BLKID_LIBRARIES})

  # this is unsatisfying and hopefully temporary; ARROW should not be
  # part of radosgw_admin
  if(WITH_RADOSGW_ARROW_FLIGHT)
    target_link_libraries(rgw-standalone-admin ${ARROW_LIBRARIES} ${ARROW_FLIGHT_LIBRARIES})
  endif(WITH_RADOSGW_ARROW_FLIGHT)

  target_compile_OPTIONS(rgw-standalone-admin
    PUBLIC "-DWITH_RADOSGW_STANDALONE"
    PUBLIC "-DWITH_RADOSGW_DBSTORE"
    PUBLIC "-UWITH_RADOSGW_D4N"
    PUBLIC "-UWITH_RADOSGW_RADOS"
    PUBLIC "-UWITH_RADOSGW_MOTR"
    PUBLIC "-UWITH_RADOSGW_DAOS")

  install(TARGETS rgw-standalone-admin DESTINATION bin)
endif()

set(radosgw_es_srcs
  rgw_es_main.cc)
add_executable(radosgw-es ${radosgw_es_srcs})
target_link_libraries(radosgw-es ${rgw_libs} librados
  cls_rgw_client cls_otp_client cls_lock_client cls_refcount_client
  cls_log_client cls_timeindex_client
  cls_version_client cls_user_client
  global ${LIB_RESOLV}
  ${CURL_LIBRARIES} ${EXPAT_LIBRARIES} ${BLKID_LIBRARIES})
if(WITH_RADOSGW_RADOS)
  target_compile_OPTIONS(radosgw-es PUBLIC "-DWITH_RADOSGW_RADOS")
endif()
install(TARGETS radosgw-es DESTINATION bin)

set(radosgw_token_srcs
  rgw_token.cc)
add_executable(radosgw-token ${radosgw_token_srcs})
target_link_libraries(radosgw-token
  legacy-option-headers
  librados
  global)
if(COMPILER_SUPPORTS_VLA_ERROR)
  target_compile_options(radosgw-token PRIVATE $<$<COMPILE_LANGUAGE:CXX>:-Werror=vla>)
endif()
if(WITH_RADOSGW_RADOS)
  target_compile_OPTIONS(radosgw-token PUBLIC "-DWITH_RADOSGW_RADOS")
endif()
install(TARGETS radosgw-token DESTINATION bin)

if(WITH_RADOSGW_RADOS)
set(radosgw_object_expirer_srcs
  rgw_object_expirer.cc)
add_executable(radosgw-object-expirer ${radosgw_object_expirer_srcs})
target_link_libraries(radosgw-object-expirer ${rgw_libs} librados
  cls_rgw_client cls_otp_client cls_lock_client cls_refcount_client
  cls_log_client cls_timeindex_client
  cls_version_client cls_user_client
  global ${LIB_RESOLV}
  ${CURL_LIBRARIES} ${EXPAT_LIBRARIES})
target_compile_OPTIONS(radosgw-object-expirer PUBLIC "-DWITH_RADOSGW_RADOS")
install(TARGETS radosgw-object-expirer DESTINATION bin)
endif()

set(radosgw_polparser_srcs
  rgw_polparser.cc)
add_executable(rgw-policy-check ${radosgw_polparser_srcs})
target_link_libraries(rgw-policy-check ${rgw_libs})
if(WITH_RADOSGW_RADOS)
  target_compile_OPTIONS(rgw-policy-check PUBLIC "-DWITH_RADOSGW_RADOS")
endif()
install(TARGETS rgw-policy-check DESTINATION bin)

set(radosgw_poltester_srcs
  rgw_poltester.cc)
add_executable(rgw-policy-test ${radosgw_poltester_srcs})
target_link_libraries(rgw-policy-test ${rgw_libs})
install(TARGETS rgw-policy-test DESTINATION bin)

set(librgw_srcs
  librgw.cc)
add_library(rgw SHARED ${librgw_srcs})

target_compile_definitions(rgw PUBLIC "-DCLS_CLIENT_HIDE_IOCTX")
target_include_directories(rgw
  PUBLIC "${CMAKE_SOURCE_DIR}/src/dmclock/support/src"
  PRIVATE "${CMAKE_SOURCE_DIR}/src/libkmip"
  PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
  PUBLIC "${CMAKE_SOURCE_DIR}/src/s3select/rapidjson/include"
  PRIVATE "${LUA_INCLUDE_DIR}")

target_link_libraries(rgw
  PRIVATE
  ${rgw_libs}
  rgw_common
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

if(WITH_RADOSGW_RADOS)
  target_compile_OPTIONS(rgw PUBLIC "-DWITH_RADOSGW_RADOS")
endif()

if(WITH_TESTS)
  add_executable(ceph_rgw_jsonparser
    rgw_jsonparser.cc)
  target_link_libraries(ceph_rgw_jsonparser
    ${rgw_libs}
    global)
  if(WITH_RADOSGW_RADOS)
    target_compile_OPTIONS(ceph_rgw_jsonparser PUBLIC "-DWITH_RADOSGW_RADOS")
  endif()

  add_executable(ceph_rgw_multiparser
    rgw_multiparser.cc)
  target_link_libraries(ceph_rgw_multiparser
    ${rgw_libs}
    global)
  if(WITH_RADOSGW_RADOS)
    target_compile_OPTIONS(ceph_rgw_multiparser PUBLIC "-DWITH_RADOSGW_RADOS")
  endif()

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
