add_library(crimson::cflags INTERFACE IMPORTED)
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
  common/tmap_helpers.cc
  common/tri_mutex.cc
  common/buffer_seastar.cc
  crush/CrushLocation.cc)

# the specialized version of ceph-common, where
#  - the logging is sent to Seastar backend
#  - and the template parameter of lock_policy is SINGLE
add_library(crimson-common STATIC
  ${PROJECT_SOURCE_DIR}/src/auth/Crypto.cc
  ${PROJECT_SOURCE_DIR}/src/common/admin_socket_client.cc
  ${PROJECT_SOURCE_DIR}/src/common/bit_str.cc
  ${PROJECT_SOURCE_DIR}/src/common/bloom_filter.cc
  ${PROJECT_SOURCE_DIR}/src/common/buffer.cc
  ${PROJECT_SOURCE_DIR}/src/common/ceph_argparse.cc
  ${PROJECT_SOURCE_DIR}/src/common/ceph_context.cc
  ${PROJECT_SOURCE_DIR}/src/common/ceph_crypto.cc
  ${PROJECT_SOURCE_DIR}/src/common/ceph_hash.cc
  ${PROJECT_SOURCE_DIR}/src/common/ceph_time.cc
  ${PROJECT_SOURCE_DIR}/src/common/ceph_strings.cc
  ${PROJECT_SOURCE_DIR}/src/common/ceph_releases.cc
  ${PROJECT_SOURCE_DIR}/src/common/cmdparse.cc
  ${PROJECT_SOURCE_DIR}/src/common/common_init.cc
  ${PROJECT_SOURCE_DIR}/src/common/compat.cc
  ${PROJECT_SOURCE_DIR}/src/common/code_environment.cc
  ${PROJECT_SOURCE_DIR}/src/common/config.cc
  ${PROJECT_SOURCE_DIR}/src/common/config_values.cc
  ${PROJECT_SOURCE_DIR}/src/common/dout.cc
  ${PROJECT_SOURCE_DIR}/src/common/entity_name.cc
  ${PROJECT_SOURCE_DIR}/src/common/environment.cc
  ${PROJECT_SOURCE_DIR}/src/common/errno.cc
  ${PROJECT_SOURCE_DIR}/src/common/escape.cc
  ${PROJECT_SOURCE_DIR}/src/common/hex.cc
  ${PROJECT_SOURCE_DIR}/src/common/fs_types.cc
  ${PROJECT_SOURCE_DIR}/src/common/ceph_json.cc
  ${PROJECT_SOURCE_DIR}/src/common/histogram.cc
  ${PROJECT_SOURCE_DIR}/src/common/hobject.cc
  ${PROJECT_SOURCE_DIR}/src/common/hostname.cc
  ${PROJECT_SOURCE_DIR}/src/common/ipaddr.cc
  ${PROJECT_SOURCE_DIR}/src/common/mempool.cc
  ${PROJECT_SOURCE_DIR}/src/common/options.cc
  ${PROJECT_SOURCE_DIR}/src/common/perf_counters.cc
  ${PROJECT_SOURCE_DIR}/src/common/perf_counters_key.cc
  ${PROJECT_SOURCE_DIR}/src/common/perf_histogram.cc
  ${PROJECT_SOURCE_DIR}/src/common/page.cc
  ${PROJECT_SOURCE_DIR}/src/common/pick_address.cc
  ${PROJECT_SOURCE_DIR}/src/common/snap_types.cc
  ${PROJECT_SOURCE_DIR}/src/common/signal.cc
  ${PROJECT_SOURCE_DIR}/src/common/str_list.cc
  ${PROJECT_SOURCE_DIR}/src/common/str_map.cc
  ${PROJECT_SOURCE_DIR}/src/common/strtol.cc
  ${PROJECT_SOURCE_DIR}/src/common/reverse.c
  ${PROJECT_SOURCE_DIR}/src/common/types.cc
  ${PROJECT_SOURCE_DIR}/src/common/utf8.c
  ${PROJECT_SOURCE_DIR}/src/common/version.cc
  ${PROJECT_SOURCE_DIR}/src/common/BackTrace.cc
  ${PROJECT_SOURCE_DIR}/src/common/ConfUtils.cc
  ${PROJECT_SOURCE_DIR}/src/common/DecayCounter.cc
  ${PROJECT_SOURCE_DIR}/src/common/HTMLFormatter.cc
  ${PROJECT_SOURCE_DIR}/src/common/Formatter.cc
  ${PROJECT_SOURCE_DIR}/src/common/Graylog.cc
  ${PROJECT_SOURCE_DIR}/src/common/Journald.cc
  ${PROJECT_SOURCE_DIR}/src/common/ostream_temp.cc
  ${PROJECT_SOURCE_DIR}/src/common/LogEntry.cc
  ${PROJECT_SOURCE_DIR}/src/common/TextTable.cc
  ${PROJECT_SOURCE_DIR}/src/common/Thread.cc
  ${PROJECT_SOURCE_DIR}/src/common/PluginRegistry.cc
  ${PROJECT_SOURCE_DIR}/src/common/RefCountedObj.cc
  ${PROJECT_SOURCE_DIR}/src/common/util.cc
  ${PROJECT_SOURCE_DIR}/src/compressor/Compressor.cc
  ${PROJECT_SOURCE_DIR}/src/crush/builder.c
  ${PROJECT_SOURCE_DIR}/src/crush/mapper.c
  ${PROJECT_SOURCE_DIR}/src/crush/crush.c
  ${PROJECT_SOURCE_DIR}/src/crush/hash.c
  ${PROJECT_SOURCE_DIR}/src/crush/CrushWrapper.cc
  ${PROJECT_SOURCE_DIR}/src/crush/CrushCompiler.cc
  ${PROJECT_SOURCE_DIR}/src/crush/CrushTester.cc
  ${PROJECT_SOURCE_DIR}/src/global/global_context.cc
  ${PROJECT_SOURCE_DIR}/src/global/pidfile.cc
  ${PROJECT_SOURCE_DIR}/src/librbd/Features.cc
  ${PROJECT_SOURCE_DIR}/src/librbd/io/IoOperations.cc
  ${PROJECT_SOURCE_DIR}/src/mgr/ServiceMap.cc
  ${PROJECT_SOURCE_DIR}/src/mds/inode_backtrace.cc
  ${PROJECT_SOURCE_DIR}/src/mds/mdstypes.cc
  ${PROJECT_SOURCE_DIR}/src/mds/cephfs_features.cc
  ${PROJECT_SOURCE_DIR}/src/mds/FSMap.cc
  ${PROJECT_SOURCE_DIR}/src/mds/FSMapUser.cc
  ${PROJECT_SOURCE_DIR}/src/mds/MDSMap.cc
  ${PROJECT_SOURCE_DIR}/src/mds/MDSAuthCaps.cc
  ${PROJECT_SOURCE_DIR}/src/msg/msg_types.cc
  ${PROJECT_SOURCE_DIR}/src/msg/Message.cc
  ${PROJECT_SOURCE_DIR}/src/mon/PGMap.cc
  ${PROJECT_SOURCE_DIR}/src/mon/MonCap.cc
  ${PROJECT_SOURCE_DIR}/src/mon/MonMap.cc
  ${PROJECT_SOURCE_DIR}/src/osd/osd_types.cc
  ${PROJECT_SOURCE_DIR}/src/osd/ECMsgTypes.cc
  ${PROJECT_SOURCE_DIR}/src/osd/HitSet.cc
  ${PROJECT_SOURCE_DIR}/src/osd/OSDMap.cc
  ${PROJECT_SOURCE_DIR}/src/osd/PGPeeringEvent.cc
  ${PROJECT_SOURCE_DIR}/src/common/scrub_types.cc
  ${PROJECT_SOURCE_DIR}/src/xxHash/xxhash.c
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

set(crimson_common_public_deps crimson::cflags)
if(WITH_JAEGER)
  list(APPEND crimson_common_public_deps jaeger_base)
endif()

if(NOT WITH_SYSTEM_BOOST)
  list(APPEND crimson_common_deps ${ZLIB_LIBRARIES})
endif()

target_link_libraries(crimson-common
  PUBLIC
    ${crimson_common_public_deps}
  PRIVATE
    crc32 arch
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
  net/FrameAssemblerV2.cc
  net/io_handler.cc
  net/Messenger.cc
  net/SocketConnection.cc
  net/SocketMessenger.cc
  net/Socket.cc
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
