include(GetGitRevisionDescription)

enable_language(C ASM)
include(GNUInstallDirs)
# for erasure and compressor plugins
set(CMAKE_INSTALL_PKGLIBDIR ${CMAKE_INSTALL_LIBDIR}/${PROJECT_NAME})
set(CMAKE_INSTALL_FULL_PKGLIBDIR ${CMAKE_INSTALL_FULL_LIBDIR}/${PROJECT_NAME})
# to be compatible with configure_files shared with autoconfig
set(bindir ${CMAKE_INSTALL_FULL_BINDIR})
set(sbindir ${CMAKE_INSTALL_FULL_SBINDIR})
set(libdir ${CMAKE_INSTALL_FULL_LIBDIR})
set(sysconfdir ${CMAKE_INSTALL_FULL_SYSCONFDIR})
set(libexecdir ${CMAKE_INSTALL_FULL_LIBEXECDIR})
set(pkgdatadir ${CMAKE_INSTALL_FULL_DATADIR})
set(datadir ${CMAKE_INSTALL_FULL_DATADIR}/${PROJECT_NAME})
set(prefix ${CMAKE_INSTALL_PREFIX})

add_definitions("-DCEPH_LIBDIR=\"${CMAKE_INSTALL_FULL_LIBDIR}\"")
add_definitions("-DCEPH_PKGLIBDIR=\"${CMAKE_INSTALL_FULL_PKGLIBDIR}\"")
add_definitions("-DHAVE_CONFIG_H -D__CEPH__ -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS")
if(LINUX)
  add_definitions("-D_GNU_SOURCE -D_FILE_OFFSET_BITS=64")
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wtype-limits -Wignored-qualifiers -Winit-self")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char")


if(CMAKE_CXX_COMPILER_ID STREQUAL GNU)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -rdynamic")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pie")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL Clang)
  set(CMAKE_EXE_LINKER_FLAGS "-Wl,-export-dynamic")
  set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -rdynamic -Wl,-export-dynamic -export-dynamic")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-inconsistent-missing-override")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-mismatched-tags -Wno-unused-function")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-local-typedef -Wno-inconsistent-missing-override")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-private-field -Wno-varargs")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-gnu-designator -Wno-mismatched-tags")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-missing-braces -Wno-parentheses -Wno-deprecated-register")
endif(CMAKE_CXX_COMPILER_ID STREQUAL GNU)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING "Default BUILD_TYPE is RelWithDebInfo, other options are: Debug, Release, and MinSizeRel." FORCE) 
endif()

if(NOT CMAKE_BUILD_TYPE STREQUAL Debug)
  # we use assert(3) for testing, so scrub the -DNDEBUG defined by default
  string(TOUPPER "${CMAKE_BUILD_TYPE}" build_type_upper)
  foreach(flags
      CMAKE_CXX_FLAGS_${build_type_upper}
      CMAKE_C_FLAGS_${build_type_upper})
    string(REGEX REPLACE "(^| )[/-]D *NDEBUG($| )" " " "${flags}" "${${flags}}")
  endforeach()
endif()

include(CheckCCompilerFlag)
if(CMAKE_CXX_COMPILER_ID STREQUAL GNU)
  CHECK_C_COMPILER_FLAG("-Wp,-U_FORTIFY_SOURCE,-D_FORTIFY_SOURCE=2" HAS_FORTIFY_SOURCE)
  if(NOT CMAKE_BUILD_TYPE STREQUAL Debug)
    if(HAS_FORTIFY_SOURCE)
      set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wp,-U_FORTIFY_SOURCE,-D_FORTIFY_SOURCE=2")
    endif()
  endif()
  CHECK_C_COMPILER_FLAG(-fstack-protector-strong HAS_STACK_PROTECT)
  if (HAS_STACK_PROTECT)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fstack-protector-strong")
  endif()
endif(CMAKE_CXX_COMPILER_ID STREQUAL GNU)

set(CMAKE_ASM_COMPILER  ${PROJECT_SOURCE_DIR}/src/yasm-wrapper)
set(CMAKE_ASM_FLAGS "-f elf64")

execute_process(
  COMMAND yasm -f elf64 ${CMAKE_SOURCE_DIR}/src/common/crc32c_intel_fast_asm.S -o /dev/null
  RESULT_VARIABLE no_yasm
  OUTPUT_QUIET)
if(no_yasm)
  message(STATUS " we do not have a modern/working yasm")
else(no_yasm)
  message(STATUS " we have a modern and working yasm")
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64")
    message(STATUS " we are x84_64")
    set(save_quiet ${CMAKE_REQUIRED_QUIET})
    set(CMAKE_REQUIRED_QUIET true)
    include(CheckCXXSourceCompiles)
    check_cxx_source_compiles("
      #if defined(__x86_64__) && defined(__ILP32__)
      #error x32
      #endif
      int main() {}
      " not_arch_x32)
    set(CMAKE_REQUIRED_QUIET ${save_quiet})
    if(not_arch_x32)
      message(STATUS " we are not x32")
      set(HAVE_GOOD_YASM_ELF64 1)
      execute_process(COMMAND yasm -f elf64 -i
        ${CMAKE_SOURCE_DIR}/src/erasure-code/isa/isa-l/include/
        ${CMAKE_SOURCE_DIR}/src/erasure-code/isa/isa-l/erasure_code/gf_vect_dot_prod_avx2.asm.s
        -o /dev/null
        RESULT_VARIABLE rc
        OUTPUT_QUIET)
      if(NOT rc)
        set(HAVE_BETTER_YASM_ELF64 1)
        message(STATUS " yasm can also build the isa-l stuff")
      endif(NOT rc)
    else(not_arch_x32)
      message(STATUS " we are x32; no yasm for you")
    endif(not_arch_x32)
  else(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64")
    message(STATUS " we are not x86_64 && !x32")
  endif(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64")
endif(no_yasm)

set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -ftemplate-depth-1024 -Wno-invalid-offsetof")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wnon-virtual-dtor")
if(CMAKE_CXX_COMPILER_ID STREQUAL GNU)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wstrict-null-sentinel -Woverloaded-virtual")
endif(CMAKE_CXX_COMPILER_ID STREQUAL GNU)

# require c++11
if(CMAKE_VERSION VERSION_LESS "3.1")
  include(CheckCXXCompilerFlag)
  CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
  if(COMPILER_SUPPORTS_CXX11)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
  else()
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support.")
  endif()
else()
  set(CMAKE_CXX_STANDARD 11)
  set(CMAKE_CXX_EXTENSIONS OFF)
endif()

## Handle diagnostics color if compiler supports them.

CHECK_C_COMPILER_FLAG("-fdiagnostics-color=always"
  COMPILER_SUPPORTS_DIAGNOSTICS_COLOR)

set(DIAGNOSTICS_COLOR "auto"
  CACHE STRING "Used if the C/C++ compiler supports the -fdiagnostics-color option. May have one of three values -- 'auto' (default), 'always', or 'never'. If set to 'always' and the compiler supports the option, 'make [...] | less -R' will make visible diagnostics colorization of compiler output.")

if(COMPILER_SUPPORTS_DIAGNOSTICS_COLOR)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-color=${DIAGNOSTICS_COLOR}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=${DIAGNOSTICS_COLOR}")
endif()

set(EXTRALIBS rt ${CMAKE_DL_LIBS} ${ATOMIC_OPS_LIBRARIES})
if(LINUX)
  set(LIB_RESOLV resolv)
  list(APPEND EXTRALIBS ${LIB_RESOLV})
endif(LINUX)

option(WITH_PROFILER "build extra profiler binaries" OFF)
if(WITH_PROFILER)
  find_package(gperftools REQUIRED)
  list(APPEND EXTRALIBS profiler)
endif(WITH_PROFILER)

if(${ENABLE_COVERAGE})
  find_program(HAVE_GCOV gcov)
  if(NOT HAVE_GCOV)
    message(FATAL_ERROR "Coverage Enabled but gcov Not Found")
  endif(NOT HAVE_GCOV)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage -O0")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
  list(APPEND EXTRALIBS gcov)
endif(${ENABLE_COVERAGE})

if(USE_NSS)
  if(NSS_FOUND)
    if(NSPR_FOUND)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${NSS_INCLUDE_DIR} -I${NSPR_INCLUDE_DIR}")
    endif(NSPR_FOUND)
  endif(NSS_FOUND)
endif(USE_NSS)

set(GCOV_PREFIX_STRIP 4)

option(ENABLE_GIT_VERSION "build Ceph with git version string" ON)
if(${ENABLE_GIT_VERSION})
  get_git_head_revision(GIT_REFSPEC CEPH_GIT_VER)
  git_describe(CEPH_GIT_NICE_VER --always)
  #if building from a source tarball via make-dist
  if(${CEPH_GIT_VER} STREQUAL "GITDIR-NOTFOUND")
    message(STATUS "Ceph/.git directory not found, parsing ${CMAKE_CURRENT_SOURCE_DIR}/.git_version for CEPH_GIT_VER and CEPH_GIT_NICE_VER")
    file(STRINGS ${CMAKE_CURRENT_SOURCE_DIR}/.git_version CEPH_GIT_SHA_AND_TAG)
    list(GET CEPH_GIT_SHA_AND_TAG 0 CEPH_GIT_VER)
    list(GET CEPH_GIT_SHA_AND_TAG 1 CEPH_GIT_NICE_VER)
  endif(${CEPH_GIT_VER} STREQUAL "GITDIR-NOTFOUND")
  # remove 'v' prefix from raw git version
  string(SUBSTRING ${CEPH_GIT_NICE_VER} 1 -1 CEPH_GIT_NICE_VER)
else(${ENABLE_GIT_VERSION})
  set(CEPH_GIT_VER "no_version")
  set(CEPH_GIT_NICE_VER "Development")
endif(${ENABLE_GIT_VERSION})

option(WITH_OCF "build OCF-compliant cluster resource agent" OFF)
if(WITH_OCF)
  add_subdirectory(ocf)
endif()

option(WITH_CEPHFS_JAVA "build libcephfs Java bindings" OFF)
if(WITH_CEPHFS_JAVA)
  add_subdirectory(java)
endif()

# Python stuff
find_package(PythonInterp 2 REQUIRED)
find_package(PythonLibs 2 REQUIRED)

option(WITH_PYTHON3 "build python3 bindings" "CHECK")
if(WITH_PYTHON3 MATCHES "check|CHECK")
  find_package(Python3Interp 3 QUIET)
  find_package(Python3Libs 3 QUIET)
  if(PYTHON3INTERP_FOUND AND PYTHON3LIBS_FOUND)
    set(WITH_PYTHON3 ON)
  else()
    set(WITH_PYTHON3 OFF)
  endif()
elseif(WITH_PYTHON3)
  find_package(Python3Interp 3 REQUIRED)
  find_package(Python3Libs 3 REQUIRED)
endif()

if(HAVE_XIO)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${XIO_INCLUDE_DIR}")
  list(APPEND EXTRALIBS ${XIO_LIBRARY} pthread rt)
endif(HAVE_XIO)

if(HAVE_RDMA)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${RDMA_INCLUDE_DIR}")
  list(APPEND EXTRALIBS ${RDMA_LIBRARIES} pthread rt)
endif(HAVE_RDMA)

if(HAVE_DPDK)
  set(CMAKE_CXX_FLAGS "-march=native ${CMAKE_CXX_FLAGS} -I${DPDK_INCLUDE_DIR}")
  list(APPEND EXTRALIBS ${DPDK_LIBRARY})
  if(NOT USE_CRYPTOPP)
    message(FATAL_ERROR "CRYPTOPP must be supported when enable DPDK.")
  endif(NOT USE_CRYPTOPP)
  list(APPEND EXTRALIBS ${DPDK_LIBRARIES})
endif(HAVE_DPDK)

# sort out which allocator to use
if(ALLOCATOR STREQUAL "tcmalloc")
  set(ALLOC_LIBS ${GPERFTOOLS_TCMALLOC_LIBRARY})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  set(TCMALLOC_srcs perfglue/heap_profiler.cc)
elseif(ALLOCATOR STREQUAL "tcmalloc_minimal")
  set(ALLOC_LIBS ${GPERFTOOLS_TCMALLOC_MINIMAL_LIBRARY})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
elseif(ALLOCATOR STREQUAL "jemalloc")
  set(ALLOC_LIBS jemalloc)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
elseif(ALLOCATOR STREQUAL "libc")
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
endif()

# tcmalloc heap profiler
set(heap_profiler_files ${TCMALLOC_srcs})
add_library(heap_profiler_objs OBJECT ${heap_profiler_files})

# Common infrastructure
configure_file(
  ${CMAKE_SOURCE_DIR}/src/ceph_ver.h.in.cmake
  ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h
  @ONLY)

set(arch_files
  arch/arm.c
  arch/intel.c
  arch/probe.cc)

set(auth_files
  auth/AuthAuthorizeHandler.cc
  auth/AuthClientHandler.cc
  auth/AuthSessionHandler.cc
  auth/AuthMethodList.cc
  auth/cephx/CephxAuthorizeHandler.cc
  auth/cephx/CephxClientHandler.cc
  auth/cephx/CephxProtocol.cc
  auth/cephx/CephxSessionHandler.cc
  auth/none/AuthNoneAuthorizeHandler.cc
  auth/unknown/AuthUnknownAuthorizeHandler.cc
  auth/Crypto.cc
  auth/KeyRing.cc
  auth/RotatingKeyRing.cc)

set(mds_files)
list(APPEND mds_files
  mds/MDSMap.cc
  mds/FSMap.cc
  mds/FSMapUser.cc
  mds/inode_backtrace.cc
  mds/mdstypes.cc
  mds/flock.cc)

set(crush_srcs
  crush/builder.c
  crush/mapper.c
  crush/crush.c
  crush/hash.c
  crush/CrushWrapper.cc
  crush/CrushCompiler.cc
  crush/CrushTester.cc
  crush/CrushLocation.cc)

add_library(crush STATIC ${crush_srcs})

add_subdirectory(json_spirit)

include_directories("${CMAKE_SOURCE_DIR}/src/xxHash")

set(xio_common_srcs)
if(HAVE_XIO)
  list(APPEND xio_common_srcs
    msg/xio/XioConnection.cc
    msg/xio/XioMsg.cc
    msg/xio/XioPool.cc
    msg/xio/XioMessenger.cc
    msg/xio/XioPortal.cc
    msg/xio/QueueStrategy.cc)
endif(HAVE_XIO)

set(async_rdma_common_srcs)
if(HAVE_RDMA)
  list(APPEND async_rdma_common_srcs
    msg/async/rdma/Infiniband.cc
    msg/async/rdma/RDMAConnectedSocketImpl.cc
    msg/async/rdma/RDMAServerSocketImpl.cc
    msg/async/rdma/RDMAStack.cc)
endif(HAVE_RDMA)

set(dpdk_common_srcs)
if(HAVE_DPDK)
  list(APPEND dpdk_common_srcs
    msg/async/dpdk/ARP.cc
    msg/async/dpdk/DPDK.cc
    msg/async/dpdk/dpdk_rte.cc
    msg/async/dpdk/DPDKStack.cc
    msg/async/dpdk/EventDPDK.cc
    msg/async/dpdk/IP.cc
    msg/async/dpdk/net.cc
    msg/async/dpdk/IPChecksum.cc
    msg/async/dpdk/Packet.cc
    msg/async/dpdk/TCP.cc
    msg/async/dpdk/UserspaceEvent.cc
    msg/async/dpdk/ethernet.cc)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${DPDK_INCLUDE_DIR}")
endif(HAVE_DPDK)

if(HAVE_GOOD_YASM_ELF64)
  set(yasm_srcs
    common/crc32c_intel_fast_asm.S
    common/crc32c_intel_fast_zero_asm.S)
endif(HAVE_GOOD_YASM_ELF64)

add_library(common_buffer_obj OBJECT
  common/buffer.cc)

add_library(common_texttable_obj OBJECT
  common/TextTable.cc)

set(libcommon_files
  ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h
  ceph_ver.c
  common/AsyncOpTracker.cc
  common/DecayCounter.cc
  common/LogClient.cc
  common/LogEntry.cc
  common/PrebufferedStreambuf.cc
  common/BackTrace.cc
  common/perf_counters.cc
  common/mutex_debug.cc
  common/Mutex.cc
  common/OutputDataSocket.cc
  common/admin_socket.cc
  common/admin_socket_client.cc
  common/bloom_filter.cc
  common/Readahead.cc
  ${crush_srcs}
  common/cmdparse.cc
  common/escape.c
  common/io_priority.cc
  common/Clock.cc
  common/ceph_time.cc
  common/mempool.cc
  common/Throttle.cc
  common/Timer.cc
  common/Finisher.cc
  common/environment.cc
  common/sctp_crc32.c
  common/crc32c.cc
  common/crc32c_intel_baseline.c
  common/crc32c_intel_fast.c
  ${yasm_srcs}
  xxHash/xxhash.c
  common/assert.cc
  common/run_cmd.cc
  common/WorkQueue.cc
  common/ConfUtils.cc
  common/MemoryModel.cc
  common/fd.cc
  common/xattr.c
  common/str_list.cc
  common/str_map.cc
  common/snap_types.cc
  common/errno.cc
  common/TrackedOp.cc
  common/SloppyCRCMap.cc
  common/types.cc
  $<TARGET_OBJECTS:common_buffer_obj>
  $<TARGET_OBJECTS:common_texttable_obj>
  log/Log.cc
  log/SubsystemMap.cc
  mon/MonCap.cc
  mon/MonClient.cc
  mon/MonMap.cc
  mgr/MgrClient.cc
  msg/simple/Accepter.cc
  msg/DispatchQueue.cc
  msg/Message.cc
  osd/ECMsgTypes.cc
  osd/HitSet.cc
  common/RefCountedObj.cc
  msg/Messenger.cc
  msg/simple/Pipe.cc
  msg/simple/PipeConnection.cc
  msg/simple/SimpleMessenger.cc
  msg/async/AsyncConnection.cc
  msg/async/AsyncMessenger.cc
  msg/async/Event.cc
  msg/async/EventSelect.cc
  msg/async/Stack.cc
  msg/async/PosixStack.cc
  msg/async/net_handler.cc
  ${xio_common_srcs}
  ${async_rdma_common_srcs}
  ${dpdk_common_srcs}
  msg/msg_types.cc
  common/hobject.cc
  osd/OSDMap.cc
  common/histogram.cc
  osd/osd_types.cc
  common/blkdev.cc
  common/common_init.cc
  common/pipe.c
  common/ceph_argparse.cc
  common/ceph_context.cc
  common/code_environment.cc
  common/dout.cc
  common/signal.cc
  common/simple_spin.cc
  common/Thread.cc
  common/Formatter.cc
  common/HTMLFormatter.cc
  common/HeartbeatMap.cc
  common/PluginRegistry.cc
  common/ceph_fs.cc
  common/ceph_hash.cc
  common/ceph_strings.cc
  common/ceph_frag.cc
  common/config.cc
  common/utf8.c
  common/mime.c
  common/strtol.cc
  common/page.cc
  common/lockdep.cc
  common/version.cc
  common/hex.cc
  common/entity_name.cc
  common/ceph_crypto.cc
  common/ceph_crypto_cms.cc
  common/ceph_json.cc
  common/ipaddr.cc
  common/pick_address.cc
  common/address_helper.cc
  common/linux_version.c
  common/TracepointProvider.cc
  common/Cycles.cc
  common/scrub_types.cc
  common/bit_str.cc
  osdc/Striper.cc
  osdc/Objecter.cc
  common/Graylog.cc
  common/fs_types.cc
  common/dns_resolve.cc
  ${arch_files}
  ${auth_files}
  $<TARGET_OBJECTS:compressor_objs>
  ${mds_files})
if(LINUX)
  list(APPEND libcommon_files msg/async/EventEpoll.cc)
  message(STATUS " Using EventEpoll for events.")
elseif(FREEBSD OR APPLE)
  list(APPEND libcommon_files msg/async/EventKqueue.cc)
  message(STATUS " Using EventKqueue for events.")
endif(LINUX)

set(mon_common_files
  auth/AuthSessionHandler.cc
  auth/cephx/CephxSessionHandler.cc
  erasure-code/ErasureCodePlugin.cc)
add_library(mon_common_objs OBJECT ${mon_common_files})
set(common_mountcephfs_files
  common/armor.c
  common/safe_io.c
  common/module.c
  common/addr_parsing.c)
add_library(common_mountcephfs_objs OBJECT
  ${common_mountcephfs_files})

if(WITH_PROFILER)
  list(APPEND libcommon_files
    perfglue/cpu_profiler.cc)
else()
  list(APPEND libcommon_files
    perfglue/disabled_stubs.cc)
endif()

if(ENABLE_SHARED)
  list(APPEND libcommon_files
    $<TARGET_OBJECTS:global_common_objs>)
endif(ENABLE_SHARED)

add_library(common STATIC ${libcommon_files}
  $<TARGET_OBJECTS:mon_common_objs>
  $<TARGET_OBJECTS:common_mountcephfs_objs>)
target_link_libraries(common json_spirit erasure_code rt ${LIB_RESOLV}
  ${Boost_LIBRARIES} ${BLKID_LIBRARIES} ${Backtrace_LIBRARIES}
  ${CRYPTO_LIBS} ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS} ${URCU_LIBRARIES})

set_source_files_properties(${CMAKE_SOURCE_DIR}/src/ceph_ver.c
  ${CMAKE_SOURCE_DIR}/src/common/version.cc
  ${CMAKE_SOURCE_DIR}/src/test/encoding/ceph_dencoder.cc
  APPEND PROPERTY OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h)

include(SIMDExt)
if(HAVE_ARMV8_CRC)
  add_library(common_crc_aarch64 STATIC common/crc32c_aarch64.c)
  target_link_libraries(common common_crc_aarch64)
endif(HAVE_ARMV8_CRC)

add_library(common_utf8 STATIC common/utf8.c)

if(${WITH_LTTNG})
  add_subdirectory(tracing)
endif(${WITH_LTTNG})

add_subdirectory(global)

add_subdirectory(lua)

# rados object classes
add_subdirectory(cls)

# RADOS client/library
add_subdirectory(osdc)

add_library(rados_snap_set_diff_obj OBJECT librados/snap_set_diff.cc)

add_subdirectory(include)
add_subdirectory(librados)
add_subdirectory(libradosstriper)

if (WITH_MGR)
  set(mgr_srcs
      ceph_mgr.cc
      mon/PGMap.cc
      mgr/DaemonState.cc
      mgr/DaemonServer.cc
      mgr/ClusterState.cc
      mgr/PyModules.cc
      mgr/PyFormatter.cc
      mgr/PyState.cc
      mgr/MgrPyModule.cc
      mgr/MgrStandby.cc
      mgr/Mgr.cc)
  add_executable(ceph-mgr ${mgr_srcs}
                 $<TARGET_OBJECTS:heap_profiler_objs>)
  target_include_directories(ceph-mgr PRIVATE "${PYTHON_INCLUDE_DIRS}")
  target_link_libraries(ceph-mgr mds osdc global
      -lboost_python ${PYTHON_LIBRARIES} ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})
  install(TARGETS ceph-mgr DESTINATION bin)
endif (WITH_MGR)

set(librados_config_srcs
  librados-config.cc)
add_executable(librados-config ${librados_config_srcs})
target_link_libraries(librados-config librados global ${BLKID_LIBRARIES} ${RDMA_LIBRARIES}
  ${CMAKE_DL_LIBS})

install(TARGETS librados-config DESTINATION bin)

# virtualenv base directory for ceph-disk and ceph-detect-init
set(CEPH_BUILD_VIRTUALENV $ENV{TMPDIR})
if(NOT CEPH_BUILD_VIRTUALENV)
  set(CEPH_BUILD_VIRTUALENV /tmp)
endif()

add_subdirectory(pybind)
add_subdirectory(ceph-disk)
add_subdirectory(ceph-detect-init)

## dencoder
CHECK_C_COMPILER_FLAG("-fvar-tracking-assignments" HAS_VTA)
if(HAS_VTA)
  set_source_files_properties(test/encoding/ceph_dencoder.cc
    PROPERTIES COMPILE_FLAGS -fno-var-tracking-assignments)
endif()

set(dencoder_srcs
  test/encoding/ceph_dencoder.cc
  $<TARGET_OBJECTS:common_texttable_obj>
  )
if(${WITH_RADOSGW})
  list(APPEND dencoder_srcs
    rgw/rgw_dencoder.cc
  )
  set(DENCODER_EXTRALIBS
    rgw_a
    cls_rgw_client
  )
endif(${WITH_RADOSGW})
if(WITH_RBD)
  set(DENCODER_EXTRALIBS
      ${DENCODER_EXTRALIBS}
      rbd_replay_types)
  if(LINUX)
    list(APPEND dencoder_srcs
      $<TARGET_OBJECTS:krbd_objs>
      $<TARGET_OBJECTS:parse_secret_objs>)
    set(DENCODER_EXTRALIBS
      ${DENCODER_EXTRALIBS}
      keyutils
      udev)
  endif(LINUX)
endif(WITH_RBD)

add_executable(ceph-dencoder ${dencoder_srcs})
target_link_libraries(ceph-dencoder
  global
  os
  osd
  mds
  mon
  journal
  ${DENCODER_EXTRALIBS}
  cls_lock_client
  cls_refcount_client
  cls_log_client
  cls_statelog_client
  cls_version_client
  cls_replica_log_client
  cls_user_client
  cls_journal_client
  cls_timeindex_client
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS ceph-dencoder DESTINATION bin)

# Monitor
set(common_util_src
  common/util.cc)
add_library(common_util_obj OBJECT ${common_util_src})

add_subdirectory(mon)
set(ceph_mon_srcs
  ceph_mon.cc)
add_executable(ceph-mon ${ceph_mon_srcs}
  $<TARGET_OBJECTS:common_texttable_obj>)
add_dependencies(ceph-mon erasure_code_plugins)
      target_link_libraries(ceph-mon mon common os global ${EXTRALIBS}
  ${CMAKE_DL_LIBS})
install(TARGETS ceph-mon DESTINATION bin)

# OSD/ObjectStore
# make rocksdb statically

set(ROCKSDB_CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=ON)

if(ALLOCATOR STREQUAL "jemalloc")
  list(APPEND ROCKSDB_CMAKE_ARGS -DWITH_JEMALLOC=ON)
endif()

if (WITH_CCACHE AND CCACHE_FOUND)
  list(APPEND ROCKSDB_CMAKE_ARGS -DCMAKE_CXX_COMPILER=ccache)
  list(APPEND ROCKSDB_CMAKE_ARGS -DCMAKE_CXX_COMPILER_ARG1=${CMAKE_CXX_COMPILER})
else(WITH_CCACHE AND CCACHE_FOUND)
  list(APPEND ROCKSDB_CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})
endif(WITH_CCACHE AND CCACHE_FOUND)

list(APPEND ROCKSDB_CMAKE_ARGS -DCMAKE_AR=${CMAKE_AR})
list(APPEND ROCKSDB_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})

# we use an external project and copy the sources to bin directory to ensure
# that object files are built outside of the source tree.
include(ExternalProject)
ExternalProject_Add(rocksdb_ext
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb
  CMAKE_ARGS ${ROCKSDB_CMAKE_ARGS}
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/rocksdb
  BUILD_COMMAND $(MAKE) rocksdblib
  INSTALL_COMMAND "true")

# force rocksdb make to be called on each time
ExternalProject_Add_Step(rocksdb_ext forcebuild
  DEPENDEES configure
  DEPENDERS build
  COMMAND "true"
  ALWAYS 1)

set(ROCKSDB_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb/include)

add_library(rocksdb STATIC IMPORTED)
add_dependencies(rocksdb rocksdb_ext)
set_property(TARGET rocksdb PROPERTY IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/rocksdb/librocksdblib.a")

add_subdirectory(kv)
add_subdirectory(os)

set(cls_references_files objclass/class_api.cc)
add_library(cls_references_objs OBJECT ${cls_references_files})

add_subdirectory(osd)
set(ceph_osd_srcs
  ceph_osd.cc)
add_executable(ceph-osd ${ceph_osd_srcs}
  $<TARGET_OBJECTS:common_util_obj>)
add_dependencies(ceph-osd erasure_code_plugins)
target_link_libraries(ceph-osd osd os global ${BLKID_LIBRARIES} ${RDMA_LIBRARIES})
if(WITH_FUSE)
  target_link_libraries(ceph-osd ${FUSE_LIBRARIES})
endif()
install(TARGETS ceph-osd DESTINATION bin)

add_subdirectory(mds)
set(ceph_mds_srcs
  ceph_mds.cc)
add_executable(ceph-mds ${ceph_mds_srcs}
  $<TARGET_OBJECTS:common_util_obj>)
target_link_libraries(ceph-mds mds ${CMAKE_DL_LIBS} global
  ${Boost_THREAD_LIBRARY})
install(TARGETS ceph-mds DESTINATION bin)

add_subdirectory(erasure-code)

# Support/Tools
if(WITH_TESTS)
add_subdirectory(googletest/googlemock)
add_subdirectory(test)
endif(WITH_TESTS)

add_subdirectory(compressor)
add_subdirectory(tools)

if(WITH_TESTS)
configure_file(${CMAKE_SOURCE_DIR}/src/ceph-coverage.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-coverage @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/ceph-debugpack.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-debugpack @ONLY)
endif(WITH_TESTS)

configure_file(${CMAKE_SOURCE_DIR}/src/ceph.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/ceph-crush-location.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-crush-location @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/init-ceph.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/init-ceph @ONLY)

configure_file(ceph-post-file.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-post-file @ONLY)

if(WITH_TESTS)
install(PROGRAMS
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-debugpack
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-coverage
  DESTINATION bin)
endif(WITH_TESTS)

install(PROGRAMS
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-crush-location
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-post-file
  ${CMAKE_SOURCE_DIR}/src/ceph-run
  ${CMAKE_SOURCE_DIR}/src/ceph-rest-api
  ${CMAKE_SOURCE_DIR}/src/ceph-clsinfo
  DESTINATION bin)
install(PROGRAMS
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/init-ceph
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/init.d
  RENAME ceph)

install(FILES
  ${CMAKE_SOURCE_DIR}/share/id_rsa_drop.ceph.com
  ${CMAKE_SOURCE_DIR}/share/id_rsa_drop.ceph.com.pub
  ${CMAKE_SOURCE_DIR}/share/known_hosts_drop.ceph.com
  DESTINATION ${CMAKE_INSTALL_DATADIR}/ceph)

install(PROGRAMS
  ceph_common.sh
  ceph-osd-prestart.sh
  DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/ceph)

install(PROGRAMS
  ${CMAKE_SOURCE_DIR}/src/ceph-create-keys
#  ${CMAKE_SOURCE_DIR}/src/ceph-disk
  ${CMAKE_SOURCE_DIR}/src/ceph-disk-udev
  DESTINATION sbin)

add_subdirectory(bash_completion)

if(KEYUTILS_FOUND)
  set(parse_secret_files
    common/secret.c)
  add_library(parse_secret_objs OBJECT ${parse_secret_files})
endif()

if(WITH_LIBCEPHFS)
  add_subdirectory(client)
  set(libcephfs_srcs libcephfs.cc)
  add_library(cephfs ${CEPH_SHARED} ${libcephfs_srcs})
  target_link_libraries(cephfs LINK_PRIVATE client
    ${CRYPTO_LIBS} ${EXTRALIBS})
  if(ENABLE_SHARED)
    foreach(name common client osdc)
      set(CEPHFS_LINK_FLAGS "${CEPHFS_LINK_FLAGS} -Wl,--exclude-libs,lib${name}.a")
    endforeach()
    set_target_properties(cephfs PROPERTIES
      OUTPUT_NAME cephfs
      VERSION 2.0.0
      SOVERSION 2
      LINK_FLAGS ${CEPHFS_LINK_FLAGS})
  endif(ENABLE_SHARED)
  install(TARGETS cephfs DESTINATION ${CMAKE_INSTALL_LIBDIR})
  install(DIRECTORY
    "${CMAKE_SOURCE_DIR}/src/include/cephfs"
    DESTINATION include)
  set(ceph_syn_srcs
    ceph_syn.cc
    client/SyntheticClient.cc)
  add_executable(ceph-syn ${ceph_syn_srcs})
  target_link_libraries(ceph-syn client global)

  set(mount_ceph_srcs
    mount/mount.ceph.c)
  add_executable(mount.ceph ${mount_ceph_srcs}
    $<TARGET_OBJECTS:parse_secret_objs>
    $<TARGET_OBJECTS:common_mountcephfs_objs>)
  target_link_libraries(mount.ceph keyutils)

  install(TARGETS ceph-syn DESTINATION bin)
  install(TARGETS mount.ceph DESTINATION ${CMAKE_INSTALL_SBINDIR})

  if(WITH_FUSE)
    set(ceph_fuse_srcs
      ceph_fuse.cc
      client/fuse_ll.cc)
    add_executable(ceph-fuse ${ceph_fuse_srcs})
    target_link_libraries(ceph-fuse ${ALLOC_LIBS} ${FUSE_LIBRARIES} client global)
    set_target_properties(ceph-fuse PROPERTIES COMPILE_FLAGS "-I${FUSE_INCLUDE_DIRS}")
    install(TARGETS ceph-fuse DESTINATION bin)
    install(PROGRAMS mount.fuse.ceph DESTINATION ${CMAKE_INSTALL_SBINDIR})
  endif(WITH_FUSE)
endif(WITH_LIBCEPHFS)

add_subdirectory(journal)

if(LINUX)
  add_library(krbd_objs OBJECT krbd.cc)
endif()

if(${WITH_RBD})
  add_subdirectory(librbd)
  if(WITH_FUSE)
    add_subdirectory(rbd_fuse)
  endif()

  install(PROGRAMS
    ${CMAKE_SOURCE_DIR}/src/ceph-rbdnamer
    ${CMAKE_SOURCE_DIR}/src/rbd-replay-many
    ${CMAKE_SOURCE_DIR}/src/rbdmap
    DESTINATION ${CMAKE_INSTALL_BINDIR})
  add_subdirectory(rbd_replay)
endif(${WITH_RBD})

# RadosGW
if(WITH_KVS)
  add_subdirectory(key_value_store)
endif(WITH_KVS)

if(WITH_RADOSGW)
  set(civetweb_common_files civetweb/src/civetweb.c)
  add_library(civetweb_common_objs OBJECT ${civetweb_common_files})
  target_include_directories(civetweb_common_objs PUBLIC
	"${CMAKE_SOURCE_DIR}/src/civetweb/include")
  set_property(TARGET civetweb_common_objs
    APPEND PROPERTY COMPILE_DEFINITIONS USE_IPV6=1)
  if(HAVE_SSL)
    set_property(TARGET civetweb_common_objs
      APPEND PROPERTY COMPILE_DEFINITIONS NO_SSL_DL=1)
    target_include_directories(civetweb_common_objs PUBLIC
      "${SSL_INCLUDE_DIR}")
  endif(HAVE_SSL)
  add_subdirectory(rgw)
endif(WITH_RADOSGW)

install(FILES
  sample.ceph.conf
  DESTINATION ${CMAKE_INSTALL_DOCDIR})
install(FILES
  fetch_config
  DESTINATION ${CMAKE_INSTALL_DOCDIR}
  RENAME sample.fetch_config)

# Now create a useable config.h
configure_file(
  ${CMAKE_SOURCE_DIR}/src/include/config-h.in.cmake
  ${CMAKE_BINARY_DIR}/include/acconfig.h
)

add_subdirectory(brag)

# Everything you need to spin up a cluster with vstart.sh
add_custom_target(vstart DEPENDS
    ceph-osd
    ceph-mon
    ceph-mgr
    ceph-authtool
    ceph-conf
    monmaptool
    crushtool
    cython_rados
    ceph-mds)
if(WITH_RADOSGW)
  add_dependencies(vstart radosgw radosgw-admin)
endif(WITH_RADOSGW)

# Everything you need to run CephFS tests
add_custom_target(cephfs_testing DEPENDS
    vstart
    rados
    cython_modules
    cephfs
    cls_cephfs
    ceph-fuse
    ceph-dencoder
    cephfs-journal-tool
    cephfs-data-scan
    cephfs-table-tool)

if (IS_DIRECTORY "${PROJECT_SOURCE_DIR}/.git")
  add_custom_target(
    git-update
    COMMAND git submodule sync
    COMMAND git submodule update --force --init --recursive
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}")
endif()

add_subdirectory(script)
