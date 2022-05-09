add_library(crimson::cflags INTERFACE IMPORTED GLOBAL)
set(crimson_cflag_definitions "WITH_SEASTAR=1")
# disable concepts to address https://github.com/boostorg/asio/issues/312
if((CMAKE_CXX_COMPILER_ID STREQUAL GNU AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 10) OR
   (CMAKE_CXX_COMPILER_ID STREQUAL Clang))
 list(APPEND crimson_cflag_definitions
   "BOOST_ASIO_DISABLE_CONCEPTS")
endif()
set_target_properties(crimson::cflags PROPERTIES
  INTERFACE_COMPILE_DEFINITIONS "${crimson_cflag_definitions}"
  INTERFACE_COMPILE_OPTIONS $<$<COMPILE_LANGUAGE:CXX>:-Wno-non-virtual-dtor>
  INTERFACE_LINK_LIBRARIES Seastar::seastar)

set(crimson_common_srcs
  common/assert.cc
  common/buffer_io.cc
  common/config_proxy.cc
  common/fatal_signal.cc
  common/formatter.cc
  common/perf_counters_collection.cc
  common/log.cc
  common/logclient.cc
  common/operation.cc
  common/throttle.cc
  common/tri_mutex.cc)

# Components of vanilla ceph imported to crimson.
# These sources will be removed from a ceph-common library to form ceph-common-minor library.
# The ceph-common-minor library is used when making crimson binaries, that links also crimson-common.
# The ceph-common library is used when making non-crimson binaries.
set(crimson_ceph_imported_srcs
  auth/Crypto.cc
  common/admin_socket_client.cc
  common/bit_str.cc
  common/bloom_filter.cc
  common/buffer.cc
  common/buffer_seastar.cc
  common/ceph_argparse.cc
  common/ceph_context.cc
  common/ceph_crypto.cc
  common/ceph_hash.cc
  common/ceph_time.cc
  common/ceph_strings.cc
  common/ceph_releases.cc
  common/cmdparse.cc
  common/compat.cc
  common/code_environment.cc
  common/config_values.cc
  common/dout.cc
  common/entity_name.cc
  common/environment.cc
  common/errno.cc
  common/escape.cc
  common/hex.cc
  common/fs_types.cc
  common/ceph_json.cc
  common/histogram.cc
  common/hobject.cc
  common/hostname.cc
  common/ipaddr.cc
  common/mempool.cc
  common/options.cc
  common/perf_counters.cc
  common/perf_histogram.cc
  common/page.cc
  common/pick_address.cc
  common/snap_types.cc
  common/signal.cc
  common/str_list.cc
  common/str_map.cc
  common/strtol.cc
  common/reverse.c
  common/types.cc
  common/utf8.c
  common/version.cc
  common/BackTrace.cc
  common/ConfUtils.cc
  common/DecayCounter.cc
  common/HTMLFormatter.cc
  common/Formatter.cc
  common/Graylog.cc
  common/Journald.cc
  common/ostream_temp.cc
  common/LogEntry.cc
  common/TextTable.cc
  common/Thread.cc
  common/PluginRegistry.cc
  common/RefCountedObj.cc
  common/util.cc
  compressor/Compressor.cc
  crush/builder.c
  crush/mapper.c
  crush/crush.c
  crush/hash.c
  crush/CrushWrapper.cc
  crush/CrushCompiler.cc
  crush/CrushTester.cc
  crush/CrushLocation.cc
  global/global_context.cc
  global/pidfile.cc
  librbd/Features.cc
  librbd/io/IoOperations.cc
  log/Log.cc
  mgr/ServiceMap.cc
  mds/inode_backtrace.cc
  mds/mdstypes.cc
  mds/cephfs_features.cc
  mds/FSMap.cc
  mds/FSMapUser.cc
  mds/MDSMap.cc
  msg/msg_types.cc
  msg/Message.cc
  mon/PGMap.cc
  mon/MonCap.cc
  mon/MonMap.cc
  osd/osd_types.cc
  osd/ECMsgTypes.cc
  osd/HitSet.cc
  osd/OSDMap.cc
  osd/PGPeeringEvent.cc
  xxHash/xxhash.c
)

#Sources that are compiled for both libcrimson-common libceph-common.
#This is necessary when we replace ceph:: classes with crimson:: counterparts
set(crimson_ceph_used_twice_srcs
  common/config.cc
  common/common_init.cc
)

#Switch name from "imported" to "exported" to make better sense for other modules.
set(crimson_ceph_exported_srcs "${crimson_ceph_imported_srcs}" PARENT_SCOPE)

#cuts out from variable 'local_srcs' values that are present in 'exported_srcs'
#it selects only those that match 'filter', removing 'filter' prefix
function(remove_exported local_srcs filter exported_srcs)
  set(to_remove ${${exported_srcs}})
  list(FILTER to_remove INCLUDE REGEX ${filter})
  list(TRANSFORM to_remove REPLACE ${filter} "")
  list(REMOVE_ITEM ${local_srcs} ${to_remove})
endfunction()

#we compile from crimson subdir, so we need to prepend full path
list(TRANSFORM crimson_ceph_imported_srcs PREPEND "${PROJECT_SOURCE_DIR}/src/")
list(TRANSFORM crimson_ceph_used_twice_srcs PREPEND "${PROJECT_SOURCE_DIR}/src/")

# the specialized version of ceph-common, where
#  - the logging is sent to Seastar backend
#  - and the template parameter of lock_policy is SINGLE
add_library(crimson-common STATIC
  ${crimson_ceph_imported_srcs}
  ${crimson_ceph_used_twice_srcs}
  ${crimson_common_srcs}
  $<TARGET_OBJECTS:common_mountcephfs_objs>
  $<TARGET_OBJECTS:common-options-objs>)

target_compile_definitions(crimson-common PRIVATE
  "CMAKE_INSTALL_LIBDIR=\"${CMAKE_INSTALL_LIBDIR}\""
  "CEPH_INSTALL_FULL_PKGLIBDIR=\"${CEPH_INSTALL_FULL_PKGLIBDIR}\""
  "CEPH_INSTALL_DATADIR=\"${CEPH_INSTALL_DATADIR}\"")

set(crimson_common_deps
  Boost::iostreams
  Boost::random
  json_spirit)

if(WITH_JAEGER)
  include_directories(SYSTEM ${CMAKE_BINARY_DIR}/external/include)
  list(APPEND crimson_common_deps ${jaeger_base})
endif()

if(NOT WITH_SYSTEM_BOOST)
  list(APPEND crimson_common_deps ${ZLIB_LIBRARIES})
endif()

target_link_libraries(crimson-common
  PUBLIC
    crimson::cflags
  PRIVATE
    crc32
    ${crimson_common_deps}
    OpenSSL::Crypto)

set(crimson_auth_srcs
  auth/KeyRing.cc
  ${PROJECT_SOURCE_DIR}/src/auth/AuthClientHandler.cc
  ${PROJECT_SOURCE_DIR}/src/auth/AuthMethodList.cc
  ${PROJECT_SOURCE_DIR}/src/auth/AuthRegistry.cc
  ${PROJECT_SOURCE_DIR}/src/auth/AuthSessionHandler.cc
  ${PROJECT_SOURCE_DIR}/src/auth/Crypto.cc
  ${PROJECT_SOURCE_DIR}/src/auth/KeyRing.cc
  ${PROJECT_SOURCE_DIR}/src/auth/RotatingKeyRing.cc
  ${PROJECT_SOURCE_DIR}/src/auth/cephx/CephxAuthorizeHandler.cc
  ${PROJECT_SOURCE_DIR}/src/auth/cephx/CephxClientHandler.cc
  ${PROJECT_SOURCE_DIR}/src/auth/cephx/CephxProtocol.cc
  ${PROJECT_SOURCE_DIR}/src/auth/cephx/CephxSessionHandler.cc
  ${PROJECT_SOURCE_DIR}/src/auth/none/AuthNoneAuthorizeHandler.cc)
set(crimson_mgr_srcs
  mgr/client.cc)
set(crimson_mon_srcs
  mon/MonClient.cc
  ${PROJECT_SOURCE_DIR}/src/mon/MonSub.cc)
set(crimson_net_srcs
  ${PROJECT_SOURCE_DIR}/src/msg/async/crypto_onwire.cc
  ${PROJECT_SOURCE_DIR}/src/msg/async/compression_onwire.cc
  ${PROJECT_SOURCE_DIR}/src/msg/async/frames_v2.cc
  net/Errors.cc
  net/Messenger.cc
  net/SocketConnection.cc
  net/SocketMessenger.cc
  net/Socket.cc
  net/Protocol.cc
  net/ProtocolV2.cc
  net/chained_dispatchers.cc)
add_library(crimson STATIC
  ${crimson_auth_srcs}
  ${crimson_mgr_srcs}
  ${crimson_mon_srcs}
  ${crimson_net_srcs})
target_compile_options(crimson PUBLIC
  "-ftemplate-backtrace-limit=0")
set_target_properties(crimson PROPERTIES
  JOB_POOL_COMPILE heavy_compile_job_pool)
target_link_libraries(crimson
  PUBLIC
    crimson-common
    crimson::cflags)
add_subdirectory(admin)
add_subdirectory(os)
add_subdirectory(osd)
add_subdirectory(tools)
