add_library(common_buffer_obj OBJECT
  buffer.cc)

add_library(common_texttable_obj OBJECT
  TextTable.cc)

add_library(common_prioritycache_obj OBJECT
  PriorityCache.cc)
target_link_libraries(common_prioritycache_obj legacy-option-headers)

if(WIN32)
  add_library(dlfcn_win32 STATIC win32/dlfcn.cc win32/errno.cc)
endif()

add_subdirectory(options)

set(common_srcs
  AsyncOpTracker.cc
  BackTrace.cc
  ConfUtils.cc
  Cycles.cc
  CDC.cc
  DecayCounter.cc
  FastCDC.cc
  Finisher.cc
  FixedCDC.cc
  Formatter.cc
  Graylog.cc
  HTMLFormatter.cc
  HeartbeatMap.cc
  LogClient.cc
  LogEntry.cc
  ostream_temp.cc
  OutputDataSocket.cc
  PluginRegistry.cc
  Readahead.cc
  RefCountedObj.cc
  SloppyCRCMap.cc
  Thread.cc
  Throttle.cc
  Timer.cc
  TracepointProvider.cc
  TrackedOp.cc
  WorkQueue.cc
  admin_socket.cc
  admin_socket_client.cc
  assert.cc
  bit_str.cc
  bloom_filter.cc
  ceph_argparse.cc
  ceph_context.cc
  ceph_crypto.cc
  ceph_frag.cc
  ceph_fs.cc
  ceph_hash.cc
  ceph_json.cc
  ceph_strings.cc
  ceph_releases.cc
  ceph_time.cc
  cmdparse.cc
  code_environment.cc
  common_init.cc
  compat.cc
  config.cc
  config_values.cc
  dout.cc
  entity_name.cc
  environment.cc
  errno.cc
  escape.cc
  fd.cc
  fs_types.cc
  hex.cc
  histogram.cc
  hobject.cc
  hostname.cc
  ipaddr.cc
  iso_8601.cc
  mempool.cc
  mime.c
  numa.cc
  openssl_opts_handler.cc
  options.cc
  page.cc
  perf_counters.cc
  perf_counters_cache.cc
  perf_counters_collection.cc
  perf_counters_key.cc
  perf_histogram.cc
  pick_address.cc
  random_string.cc
  reverse.c
  run_cmd.cc
  scrub_types.cc
  signal.cc
  snap_types.cc
  str_list.cc
  str_map.cc
  strtol.cc
  types.cc
  url_escape.cc
  pretty_binary.cc
  utf8.c
  util.cc
  version.cc)

if(WITH_SYSTEMD)
  list(APPEND common_srcs
    Journald.cc)
endif()

if(WITH_CEPH_DEBUG_MUTEX)
  list(APPEND common_srcs
    lockdep.cc
    mutex_debug.cc
    condition_variable_debug.cc
    shared_mutex_debug.cc)
endif()

if(WIN32)
  if(MINGW)
    set(CMAKE_MC_COMPILER x86_64-w64-mingw32-windmc)
    set(CMAKE_RC_COMPILER x86_64-w64-mingw32-windres)
  endif()

  add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/src/common/event_logging.h
    COMMAND ${CMAKE_MC_COMPILER} -b -e h -h ${CMAKE_BINARY_DIR}/src/common/
      -r ${CMAKE_BINARY_DIR}/src/common ${CMAKE_SOURCE_DIR}/src/common/win32/event_logging.mc
    COMMAND ${CMAKE_RC_COMPILER} ${CMAKE_BINARY_DIR}/src/common/event_logging.rc
      -o ${CMAKE_BINARY_DIR}/src/common/event_logging.o
    COMMAND ${CMAKE_CXX_COMPILER} -o ${CMAKE_BINARY_DIR}/bin/event_logging.dll -shared
      ${CMAKE_BINARY_DIR}/src/common/event_logging.o
    DEPENDS ${CMAKE_SOURCE_DIR}/src/common/win32/event_logging.mc)

  set_source_files_properties(${CMAKE_SOURCE_DIR}/src/common/win32/syslog.cc
    APPEND PROPERTY OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/src/common/event_logging.h)

  include_directories(SYSTEM "${CMAKE_BINARY_DIR}/src/common/")

  list(APPEND common_srcs
    win32/blkdev.cc
    win32/dns_resolve.cc
    win32/ifaddrs.cc
    win32/registry.cc
    win32/service.cc
    win32/SubProcess.cc
    win32/syslog.cc)
else()
  list(APPEND common_srcs
    blkdev.cc
    dns_resolve.cc
    linux_version.c
    SubProcess.cc)
endif()

set_source_files_properties(${CMAKE_SOURCE_DIR}/src/common/version.cc
  APPEND PROPERTY OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h)

if(HAS_VTA)
  set_source_files_properties(
    config.cc
    options.cc
    PROPERTIES COMPILE_FLAGS -fno-var-tracking-assignments)
endif()

if(FREEBSD)
  list(APPEND common_srcs freebsd_errno.cc)
elseif(APPLE)
  list(APPEND common_srcs darwin_errno.cc)
elseif(SUN)
  list(APPEND common_srcs solaris_errno.cc)
elseif(AIX)
  list(APPEND common_srcs aix_errno.cc)
elseif(WIN32)
  list(APPEND common_srcs win32/errno.cc)
  list(APPEND common_srcs win32/wstring.cc)
endif()

if(WITH_EVENTTRACE)
  list(APPEND common_srcs EventTrace.cc)
endif()

add_library(common-common-objs OBJECT
  ${common_srcs})
# Let's not rely on the default system headers and point Cmake to the
# retrieved OpenSSL location. This is especially important when cross
# compiling (e.g. targeting Windows).
target_include_directories(common-common-objs PRIVATE ${OPENSSL_INCLUDE_DIR})
# for options.cc
target_compile_definitions(common-common-objs PRIVATE
  "CMAKE_INSTALL_LIBDIR=\"${CMAKE_INSTALL_LIBDIR}\""
  "CEPH_INSTALL_FULL_PKGLIBDIR=\"${CEPH_INSTALL_FULL_PKGLIBDIR}\""
  "CEPH_INSTALL_DATADIR=\"${CEPH_INSTALL_DATADIR}\""
  $<TARGET_PROPERTY:${FMT_LIB},INTERFACE_COMPILE_DEFINITIONS>)
target_link_libraries(common-common-objs legacy-option-headers)

set(common_mountcephfs_srcs
  armor.c
  safe_io.c
  module.c
  addr_parsing.c)
add_library(common_mountcephfs_objs OBJECT
  ${common_mountcephfs_srcs})


set(crc32_srcs
  crc32c.cc
  crc32c_intel_baseline.c
  sctp_crc32.c)
if(HAVE_INTEL)
  list(APPEND crc32_srcs
    crc32c_intel_fast.c)
  if(HAVE_NASM_X64)
    set(CMAKE_ASM_FLAGS "-i ${PROJECT_SOURCE_DIR}/src/isa-l/include/ ${CMAKE_ASM_FLAGS}")
    list(APPEND crc32_srcs
      ${PROJECT_SOURCE_DIR}/src/isa-l/crc/crc32_iscsi_00.asm
      crc32c_intel_fast_zero_asm.s)
  endif(HAVE_NASM_X64)
elseif(HAVE_POWER8)
  list(APPEND crc32_srcs
    crc32c_ppc.c)
  if(HAVE_PPC64LE)
    list(APPEND crc32_srcs
      crc32c_ppc_asm.S
      crc32c_ppc_fast_zero_asm.S)
  endif(HAVE_PPC64LE)
elseif(HAVE_ARMV8_CRC)
  list(APPEND crc32_srcs
    crc32c_aarch64.c)
endif(HAVE_INTEL)

add_library(crc32 OBJECT ${crc32_srcs})

if(HAVE_ARMV8_CRC)
  set_target_properties(crc32 PROPERTIES
    COMPILE_FLAGS "${CMAKE_C_FLAGS} ${ARMV8_CRC_COMPILE_FLAGS}")
endif()
target_link_libraries(crc32
  arch)

add_library(common_utf8 STATIC utf8.c)

if(HAVE_KEYUTILS)
  set(parse_secret_srcs
    secret.c)
  add_library(parse_secret_objs OBJECT ${parse_secret_srcs})
endif()
