include(GetGitRevisionDescription)

include(GNUInstallDirs)
# for erasure and compressor plugins
set(CMAKE_INSTALL_PKGLIBDIR ${CMAKE_INSTALL_LIBDIR}/${PROJECT_NAME})
set(CMAKE_INSTALL_FULL_PKGLIBDIR ${CMAKE_INSTALL_FULL_LIBDIR}/${PROJECT_NAME})
# so libceph-common can be found
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
if(NOT CMAKE_INSTALL_RPATH)
  set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_FULL_PKGLIBDIR}")
endif()

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
add_definitions("-D_FILE_OFFSET_BITS=64")
if(LINUX)
  add_definitions("-D_GNU_SOURCE")
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wtype-limits -Wignored-qualifiers -Winit-self")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftemplate-depth-1024 -Wno-invalid-offsetof")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wnon-virtual-dtor")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unknown-pragmas")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unknown-pragmas")
if(CMAKE_CXX_COMPILER_ID STREQUAL GNU)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -rdynamic")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wstrict-null-sentinel -Woverloaded-virtual")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-new-ttp-matching")
  if(NOT WITH_OSD_INSTRUMENT_FUNCTIONS)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pie")
  endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL Clang)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_EXPORTS_C_FLAG}")
  set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -rdynamic -export-dynamic ${CMAKE_EXE_EXPORTS_C_FLAG}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-inconsistent-missing-override -Wno-mismatched-tags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-private-field")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-address-of-packed-member")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-function")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-local-typedef")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-varargs")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-gnu-designator")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-missing-braces -Wno-parentheses -Wno-deprecated-register")
  if(APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -undefined dynamic_lookup")
  endif()
endif(CMAKE_CXX_COMPILER_ID STREQUAL GNU)
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS}")

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
  CHECK_C_COMPILER_FLAG("-U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=2" HAS_FORTIFY_SOURCE)
  if(NOT CMAKE_BUILD_TYPE STREQUAL Debug)
    if(HAS_FORTIFY_SOURCE)
      set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=2")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=2")
    endif()
  endif()
  CHECK_C_COMPILER_FLAG(-fstack-protector-strong HAS_STACK_PROTECT)
  if (HAS_STACK_PROTECT)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fstack-protector-strong")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fstack-protector-strong")
  endif()
endif(CMAKE_CXX_COMPILER_ID STREQUAL GNU)

include(SIMDExt)
if(HAVE_INTEL)
  set(CMAKE_ASM_COMPILER  ${PROJECT_SOURCE_DIR}/src/yasm-wrapper)
  if(APPLE)
    set(object_format "macho64")
  else()
    set(object_format "elf64")
  endif()
  set(CMAKE_ASM_FLAGS "-f ${object_format}")
endif()

execute_process(
  COMMAND yasm -f "${object_format}" ${CMAKE_SOURCE_DIR}/src/common/crc32c_intel_fast_asm.s -o /dev/null
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
      execute_process(COMMAND yasm -f ${object_format} -i
        ${CMAKE_SOURCE_DIR}/src/isa-l/include/
        ${CMAKE_SOURCE_DIR}/src/isa-l/erasure_code/gf_vect_dot_prod_avx2.asm
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

# require c++17
if(CMAKE_VERSION VERSION_LESS "3.8")
  include(CheckCXXCompilerFlag)
  CHECK_CXX_COMPILER_FLAG("-std=c++17" COMPILER_SUPPORTS_CXX17)
  if(NOT COMPILER_SUPPORTS_CXX17)
    message(FATAL_ERROR
      "The compiler ${CMAKE_CXX_COMPILER} has no C++17 support.")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
  include(CheckCCompilerFlag)
  CHECK_C_COMPILER_FLAG("-std=gnu99" COMPILER_SUPPORTS_GNU99)
  if(NOT COMPILER_SUPPORTS_GNU99)
    message(FATAL_ERROR
      "The compiler ${CMAKE_C_COMPILER} has no GNU C99 support.")
  endif()
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=gnu99")
else()
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_EXTENSIONS OFF)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_C_STANDARD 99)
  # we use `asm()` to inline assembly, so enable the GNU extension
  set(CMAKE_C_EXTENSIONS ON)
  set(C_STANDARD_REQUIRED ON)
endif()


if(CMAKE_COMPILER_IS_GNUCXX AND
    CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.1)
  # this is not always correct, as one can use clang with libstdc++ or
  # use old gcc with new libstdc++, but it covers the most cases.
  #
  # libstdc++ 4.9 has O(n) list::size(), and its regex is buggy
  message(SEND_ERROR "performance regression is expected due to an O(n) "
    "implementation of 'std::list::size()' in libstdc++ older than 5.1.0, "
    "Please use GCC 5.1 and up.")
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

set(EXTRALIBS ${CMAKE_DL_LIBS})
if(NOT APPLE)
  list(APPEND EXTRALIBS rt)
endif()
if(LINUX OR APPLE)
  set(LIB_RESOLV resolv)
  list(APPEND EXTRALIBS ${LIB_RESOLV})
endif()

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

include_directories(${NSS_INCLUDE_DIR} ${NSPR_INCLUDE_DIR})

set(GCOV_PREFIX_STRIP 4)

# the src/.git_version file may be written out by make-dist; otherwise
# we pull the git version from .git
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

# the src/ceph_release file is 3 lines,
#   <release number, e.g. '12' for luminous>
#   <release name, e.g. 'luminous'>
#   <release type: 'dev' for x.0.z, 'rc' or x.1.z, or 'stable' or x.2.z>
# note that the release name is semi-redundant and must match CEPH_RELEASE_*
# definitions in include/rados.h and common/ceph_strings.c.
file(STRINGS ${CMAKE_CURRENT_SOURCE_DIR}/ceph_release CEPH_RELEASE_FILE)
list(GET CEPH_RELEASE_FILE 0 CEPH_RELEASE)
list(GET CEPH_RELEASE_FILE 1 CEPH_RELEASE_NAME)
list(GET CEPH_RELEASE_FILE 2 CEPH_RELEASE_TYPE)

option(WITH_OCF "build OCF-compliant cluster resource agent" OFF)
if(WITH_OCF)
  add_subdirectory(ocf)
endif()

option(WITH_CEPHFS_JAVA "build libcephfs Java bindings" OFF)
if(WITH_CEPHFS_JAVA)
  add_subdirectory(java)
endif()

# Python stuff
option(WITH_PYTHON2 "build python2 bindings" ON)
if(WITH_PYTHON2)
  find_package(PythonInterp 2 REQUIRED)
  find_package(PythonLibs 2 REQUIRED)
endif()

option(WITH_PYTHON3 "build python3 bindings" OFF)
if(WITH_PYTHON3)
  find_package(Python3Interp 3 REQUIRED)
  find_package(Python3Libs 3 REQUIRED)
endif()

# the major version of the python bindings as a dependency of other
# targets
if(WITH_PYTHON2)
  set(PY_BINDING_INFIX "")
else()
  set(PY_BINDING_INFIX 3)
endif()

if(HAVE_XIO)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${XIO_INCLUDE_DIR}")
  list(APPEND EXTRALIBS ${XIO_LIBRARY} pthread)
endif(HAVE_XIO)

if(HAVE_RDMA)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${RDMA_INCLUDE_DIR}")
  list(APPEND EXTRALIBS ${RDMA_LIBRARIES} pthread)
endif(HAVE_RDMA)

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
  set(ALLOC_LIBS ${JEMALLOC_LIBRARIES})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
elseif(ALLOCATOR STREQUAL "libc")
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
else()
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
  message(FATAL_ERROR "Unsupported allocator selected: ${ALLOCATOR}")
endif()

# tcmalloc heap profiler
set(heap_profiler_files ${TCMALLOC_srcs})
add_library(heap_profiler_objs OBJECT ${heap_profiler_files})

if (WITH_BLKIN)
  add_subdirectory(blkin/blkin-lib)
endif(WITH_BLKIN)

# Common infrastructure
configure_file(
  ${CMAKE_SOURCE_DIR}/src/ceph_ver.h.in.cmake
  ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h
  @ONLY)

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

add_library(crush_objs OBJECT ${crush_srcs})

add_subdirectory(json_spirit)

include_directories(SYSTEM "${CMAKE_SOURCE_DIR}/src/xxHash")
include_directories(SYSTEM "${CMAKE_SOURCE_DIR}/src/rapidjson/include")

set(GMOCK_INCLUDE_DIRS
  "${CMAKE_SOURCE_DIR}/src/googletest/googletest/include/gmock")
set(GTEST_INCLUDE_DIRS
  "${CMAKE_SOURCE_DIR}/src/googletest/googletest/include/gtest")

include_directories("${CMAKE_SOURCE_DIR}/src/dmclock/src")
include_directories("${CMAKE_SOURCE_DIR}/src/dmclock/support/src")

# needed for source files that friend unit tests (e.g., using FRIEND_TEST)
include_directories(SYSTEM "${CMAKE_SOURCE_DIR}/src/googletest/googletest/include")

set(xio_common_srcs)
if(HAVE_XIO)
  list(APPEND xio_common_srcs
    msg/xio/XioConnection.cc
    msg/xio/XioMsg.cc
    msg/xio/XioPool.cc
    msg/xio/XioMessenger.cc
    msg/xio/XioPortal.cc)
endif(HAVE_XIO)

set(async_rdma_common_srcs)
if(HAVE_RDMA)
  list(APPEND async_rdma_common_srcs
    msg/async/rdma/Infiniband.cc
    msg/async/rdma/RDMAConnectedSocketImpl.cc
    msg/async/rdma/RDMAServerSocketImpl.cc
    msg/async/rdma/RDMAStack.cc)
endif(HAVE_RDMA)

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
  common/CachedPrebufferedStreambuf.cc
  common/BackTrace.cc
  common/perf_counters.cc
  common/perf_histogram.cc
  common/mutex_debug.cc
  common/Mutex.cc
  common/OutputDataSocket.cc
  common/admin_socket.cc
  common/admin_socket_client.cc
  common/bloom_filter.cc
  common/Readahead.cc
  common/cmdparse.cc
  common/escape.cc
  common/url_escape.cc
  common/io_priority.cc
  common/ceph_time.cc
  common/mempool.cc
  common/Throttle.cc
  common/Timer.cc
  common/Finisher.cc
  common/environment.cc
  common/sctp_crc32.c
  common/crc32c.cc
  common/crc32c_intel_baseline.c
  xxHash/xxhash.c
  common/assert.cc
  common/run_cmd.cc
  common/WorkQueue.cc
  common/ConfUtils.cc
  common/fd.cc
  common/xattr.c
  common/str_list.cc
  common/str_map.cc
  common/snap_types.cc
  common/errno.cc
  common/TrackedOp.cc
  common/SloppyCRCMap.cc
  common/types.cc
  common/iso_8601.cc
  log/Log.cc
  mon/MonCap.cc
  mon/MonClient.cc
  mon/MonMap.cc
  mgr/MgrClient.cc
  msg/simple/Accepter.cc
  msg/DispatchQueue.cc
  msg/Message.cc
  mgr/ServiceMap.cc
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
  msg/QueueStrategy.cc
  ${xio_common_srcs}
  ${async_rdma_common_srcs}
  msg/msg_types.cc
  common/reverse.c
  common/hobject.cc
  osd/OSDMap.cc
  osd/OSDMapMapping.cc
  common/histogram.cc
  osd/osd_types.cc
  osd/OpRequest.cc
  common/blkdev.cc
  common/common_init.cc
  common/pipe.c
  common/ceph_argparse.cc
  common/ceph_context.cc
  common/code_environment.cc
  common/dout.cc
  common/signal.cc
  common/Thread.cc
  common/Formatter.cc
  common/HTMLFormatter.cc
  common/HeartbeatMap.cc
  common/PluginRegistry.cc
  common/ceph_fs.cc
  common/ceph_hash.cc
  common/ceph_strings.cc
  common/ceph_frag.cc
  common/options.cc
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
  dmclock/src/dmclock_util.cc
  dmclock/support/src/run_every.cc
  osdc/Striper.cc
  osdc/Objecter.cc
  common/Graylog.cc
  common/fs_types.cc
  common/dns_resolve.cc
  common/hostname.cc
  common/util.cc
  librbd/Features.cc
  arch/probe.cc
  ${auth_files}
  ${mds_files})

CHECK_C_COMPILER_FLAG("-fvar-tracking-assignments" HAS_VTA)
if(HAS_VTA)
  set_source_files_properties(
    common/config.cc
    common/options.cc
    PROPERTIES COMPILE_FLAGS -fno-var-tracking-assignments)
endif()

if(FREEBSD)
  list(APPEND libcommon_files common/freebsd_errno.cc)
elseif(APPLE)
  list(APPEND libcommon_files common/darwin_errno.cc)
elseif(SUN)
  list(APPEND libcommon_files common/solaris_errno.cc)
elseif(AIX)
  list(APPEND libcommon_files common/aix_errno.cc)
endif()

if(HAVE_ARM)
  list(APPEND libcommon_files arch/arm.c)
elseif(HAVE_INTEL)
  list(APPEND libcommon_files arch/intel.c)
elseif(HAVE_POWER8)
  list(APPEND libcommon_files arch/ppc.c)
endif()

if(HAVE_INTEL)
  list(APPEND libcommon_files
    common/crc32c_intel_fast.c)
  if(HAVE_GOOD_YASM_ELF64)
    list(APPEND libcommon_files
      common/crc32c_intel_fast_asm.s
      common/crc32c_intel_fast_zero_asm.s)
  endif(HAVE_GOOD_YASM_ELF64)
elseif(HAVE_POWER8)
  list(APPEND libcommon_files
    common/crc32c_ppc.c)
  if(HAVE_PPC64LE)
    list(APPEND libcommon_files
      common/crc32c_ppc_asm.S
      common/crc32c_ppc_fast_zero_asm.S)
  endif(HAVE_PPC64LE)
endif(HAVE_INTEL)

if(LINUX)
  list(APPEND libcommon_files msg/async/EventEpoll.cc)
  message(STATUS " Using EventEpoll for events.")
elseif(FREEBSD OR APPLE)
  list(APPEND libcommon_files msg/async/EventKqueue.cc)
  message(STATUS " Using EventKqueue for events.")
endif(LINUX)

if(WITH_LTTNG AND WITH_EVENTTRACE)
  message(STATUS " Using EventTrace class.")
  add_definitions("-DWITH_EVENTTRACE")
  list(APPEND libcommon_files common/EventTrace.cc)
endif()

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

add_library(common-objs OBJECT ${libcommon_files})
set(ceph_common_objs
  $<TARGET_OBJECTS:common_buffer_obj>
  $<TARGET_OBJECTS:common_texttable_obj>
  $<TARGET_OBJECTS:compressor_objs>
  $<TARGET_OBJECTS:common-objs>
  $<TARGET_OBJECTS:common_mountcephfs_objs>
  $<TARGET_OBJECTS:global_common_objs>
  $<TARGET_OBJECTS:crush_objs>)
set(ceph_common_deps
  json_spirit erasure_code ${LIB_RESOLV}
  Boost::thread
  Boost::system
  Boost::random
  Boost::program_options
  Boost::date_time
  Boost::iostreams
  ${BLKID_LIBRARIES}
  ${Backtrace_LIBRARIES}
  ${BLKIN_LIBRARIES}
  ${CRYPTO_LIBS}
  ${CMAKE_THREAD_LIBS_INIT}
  ${CMAKE_DL_LIBS})
if(HAVE_RDMA)
  list(APPEND ceph_common_deps ${RDMA_LIBRARY})
endif()
if(NOT WITH_SYSTEM_BOOST)
  list(APPEND ceph_common_deps ${ZLIB_LIBRARIES})
endif()

set_source_files_properties(${CMAKE_SOURCE_DIR}/src/ceph_ver.c
  ${CMAKE_SOURCE_DIR}/src/common/version.cc
  ${CMAKE_SOURCE_DIR}/src/test/encoding/ceph_dencoder.cc
  APPEND PROPERTY OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h)

if(HAVE_ARMV8_CRC)
  add_library(common_crc_aarch64 STATIC common/crc32c_aarch64.c)
  set_target_properties(common_crc_aarch64 PROPERTIES COMPILE_FLAGS "${CMAKE_C_FLAGS} ${ARMV8_CRC_COMPILE_FLAGS}")
  list(APPEND ceph_common_deps common_crc_aarch64)
endif(HAVE_ARMV8_CRC)

if(WITH_DPDK)
  list(APPEND common_async_dpdk_files
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
  add_library(common_async_dpdk STATIC
    ${common_async_dpdk_files})
  # to ready the dpdk header files
  add_dependencies(common_async_dpdk
    ${DPDK_LIBRARIES})
  list(APPEND ceph_common_deps common_async_dpdk)
  target_link_libraries(common_async_dpdk
    ${DPDK_LIBRARIES})
  set_target_properties(common_async_dpdk PROPERTIES
    COMPILE_FLAGS "-march=native -I${DPDK_INCLUDE_DIR}")
endif(WITH_DPDK)

add_library(common STATIC ${ceph_common_objs})
target_link_libraries(common
  PRIVATE ${ceph_common_deps})
if(WITH_STATIC_LIBSTDCXX)
  # the apps linking against libcommon are daemons also written in C++, so we
  # need to link them against libstdc++.
  target_link_libraries(common
    INTERFACE "-static-libstdc++ -static-libgcc")
endif()

add_library(ceph-common SHARED ${ceph_common_objs})
target_link_libraries(ceph-common ${ceph_common_deps})
# appease dpkg-shlibdeps
set_target_properties(ceph-common PROPERTIES
  SOVERSION 0
  INSTALL_RPATH "")
if(WITH_STATIC_LIBSTDCXX)
  # link libstdc++ into ceph-common to avoid including libstdc++ in every apps,
  # to reduce the size of the app linking against ceph-common.
  set_target_properties(ceph-common PROPERTIES
    LINK_FLAGS "-static-libstdc++ -static-libgcc")
endif()
install(TARGETS ceph-common DESTINATION ${CMAKE_INSTALL_PKGLIBDIR})

add_library(common_utf8 STATIC common/utf8.c)

if(${WITH_LTTNG})
  add_subdirectory(tracing)
  add_dependencies(common-objs oprequest-tp)
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
      mgr/ActivePyModules.cc
      mgr/OSDHealthMetricCollector.cc
      mgr/StandbyPyModules.cc
      mgr/PyModule.cc
      mgr/PyModuleRegistry.cc
      mgr/PyModuleRunner.cc
      mgr/PyFormatter.cc
      mgr/PyOSDMap.cc
      mgr/BaseMgrModule.cc
      mgr/BaseMgrStandbyModule.cc
      mgr/ActivePyModule.cc
      mgr/MgrStandby.cc
      mgr/Mgr.cc
      mgr/Gil.cc
      mgr/mgr_commands.cc)
  add_executable(ceph-mgr ${mgr_srcs}
                 $<TARGET_OBJECTS:heap_profiler_objs>)
  target_include_directories(ceph-mgr SYSTEM PRIVATE "${PYTHON_INCLUDE_DIRS}")
  target_link_libraries(ceph-mgr osdc client global-static ceph-common
      Boost::python ${MGR_PYTHON_LIBRARIES} ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${ALLOC_LIBS})
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
add_subdirectory(ceph-volume)
add_subdirectory(ceph-detect-init)

## dencoder
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
    cls_rbd_client
    rbd_mirror_types
    rbd_types
    rbd_replay_types)
  if(WITH_KRBD)
    set(DENCODER_EXTRALIBS
      ${DENCODER_EXTRALIBS}
      krbd)
  endif()
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
add_subdirectory(mon)
set(ceph_mon_srcs
  ceph_mon.cc)
add_executable(ceph-mon ${ceph_mon_srcs}
  $<TARGET_OBJECTS:common_texttable_obj>)
add_dependencies(ceph-mon erasure_code_plugins)
target_link_libraries(ceph-mon mon os global-static ceph-common
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS})
install(TARGETS ceph-mon DESTINATION bin)

# OSD/ObjectStore
# make rocksdb statically

if(NOT WITH_SYSTEM_ROCKSDB)
  include(BuildRocksDB)
  build_rocksdb()
endif(NOT WITH_SYSTEM_ROCKSDB)

include(TestBigEndian)
test_big_endian(CEPH_BIG_ENDIAN)
if(NOT CEPH_BIG_ENDIAN)
  set(CEPH_LITTLE_ENDIAN 1)
endif()

add_subdirectory(kv)
add_subdirectory(os)

set(cls_references_files objclass/class_api.cc)
add_library(cls_references_objs OBJECT ${cls_references_files})

add_subdirectory(osd)

set(ceph_osd_srcs
  ceph_osd.cc)
add_executable(ceph-osd ${ceph_osd_srcs})
add_dependencies(ceph-osd erasure_code_plugins)
target_link_libraries(ceph-osd osd os global-static ceph-common
  ${BLKID_LIBRARIES} ${RDMA_LIBRARIES})
if(WITH_FUSE)
  target_link_libraries(ceph-osd ${FUSE_LIBRARIES})
endif()
install(TARGETS ceph-osd DESTINATION bin)

add_subdirectory(mds)
set(ceph_mds_srcs
  ceph_mds.cc)
add_executable(ceph-mds ${ceph_mds_srcs})
target_link_libraries(ceph-mds mds ${CMAKE_DL_LIBS} global-static ceph-common
  Boost::thread)
install(TARGETS ceph-mds DESTINATION bin)

add_subdirectory(erasure-code)

# Support/Tools
if(WITH_TESTS)
  add_subdirectory(googletest/googlemock)
  add_subdirectory(test)
endif(WITH_TESTS)

add_subdirectory(compressor)

add_subdirectory(tools)

# dmClock (after gmock)

add_subdirectory(dmclock/src)

option(WITH_DMCLOCK_TESTS
  "enable the build of dmclock-tests and dmclock-data-struct tests binaries"
  OFF)
if(WITH_TESTS AND WITH_DMCLOCK_TESTS)
  # note: add_test is not being called, so dmclock tests aren't part
  # of ceph tests
  add_subdirectory(dmclock/test)
  add_subdirectory(dmclock/support/test)
endif(WITH_TESTS AND WITH_DMCLOCK_TESTS)

if(HAVE_INTEL AND HAVE_BETTER_YASM_ELF64 AND (NOT APPLE))
  add_subdirectory(crypto/isa-l)
endif()

if(WITH_TESTS)
  configure_file(${CMAKE_SOURCE_DIR}/src/ceph-coverage.in
    ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-coverage @ONLY)
  configure_file(${CMAKE_SOURCE_DIR}/src/ceph-debugpack.in
    ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-debugpack @ONLY)
endif()

configure_file(${CMAKE_SOURCE_DIR}/src/ceph.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/init-ceph.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/init-ceph @ONLY)

configure_file(ceph-post-file.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-post-file @ONLY)

if(WITH_TESTS)
  install(PROGRAMS
    ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-debugpack
    ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-coverage
    DESTINATION bin)
endif()

install(PROGRAMS
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-post-file
  ${CMAKE_SOURCE_DIR}/src/ceph-run
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
  DESTINATION sbin)

add_subdirectory(bash_completion)

if(WITH_LIBCEPHFS OR WITH_KRBD)
  set(parse_secret_files
    common/secret.c)
  add_library(parse_secret_objs OBJECT ${parse_secret_files})
endif()

add_subdirectory(client)

if(WITH_LIBCEPHFS)
  set(libcephfs_srcs libcephfs.cc)
  add_library(cephfs ${CEPH_SHARED} ${libcephfs_srcs})
  target_link_libraries(cephfs PRIVATE client ceph-common
    ${CRYPTO_LIBS} ${EXTRALIBS})
  if(WITH_STATIC_LIBSTDCXX)
    target_link_libraries(cephfs
      INTERFACE "-static-libstdc++ -static-libgcc")
  endif()
  if(ENABLE_SHARED)
    set_target_properties(cephfs PROPERTIES
      OUTPUT_NAME cephfs
      VERSION 2.0.0
      SOVERSION 2)
    if(NOT APPLE)
      foreach(name ceph-common client osdc)
        set_property(TARGET cephfs APPEND_STRING PROPERTY
          LINK_FLAGS " -Wl,--exclude-libs,lib${name}.a")
      endforeach()
    endif()
  endif(ENABLE_SHARED)
  install(TARGETS cephfs DESTINATION ${CMAKE_INSTALL_LIBDIR})
  install(DIRECTORY
    "${CMAKE_SOURCE_DIR}/src/include/cephfs"
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
  set(ceph_syn_srcs
    ceph_syn.cc
    client/SyntheticClient.cc)
  add_executable(ceph-syn ${ceph_syn_srcs})
  target_link_libraries(ceph-syn client global-static ceph-common)

  set(mount_ceph_srcs
    mount/mount.ceph.c)
  add_executable(mount.ceph ${mount_ceph_srcs}
    $<TARGET_OBJECTS:parse_secret_objs>
    $<TARGET_OBJECTS:common_mountcephfs_objs>)
  set_target_properties(mount.ceph PROPERTIES
    INSTALL_RPATH "")
  target_link_libraries(mount.ceph ${KEYUTILS_LIBRARIES})

  install(TARGETS ceph-syn DESTINATION bin)
  install(TARGETS mount.ceph DESTINATION ${CMAKE_INSTALL_SBINDIR})
endif(WITH_LIBCEPHFS)

if(WITH_FUSE)
  set(ceph_fuse_srcs
    ceph_fuse.cc
    client/fuse_ll.cc)
  add_executable(ceph-fuse ${ceph_fuse_srcs})
  target_link_libraries(ceph-fuse ${ALLOC_LIBS} ${FUSE_LIBRARIES}
    client ceph-common global-static)
  set_target_properties(ceph-fuse PROPERTIES COMPILE_FLAGS "-I${FUSE_INCLUDE_DIRS}")
  install(TARGETS ceph-fuse DESTINATION bin)
  install(PROGRAMS mount.fuse.ceph DESTINATION ${CMAKE_INSTALL_SBINDIR})
endif(WITH_FUSE)

add_subdirectory(journal)

if(WITH_RBD)
  if(WITH_KRBD)
    add_library(krbd STATIC krbd.cc
      $<TARGET_OBJECTS:parse_secret_objs>)
    target_link_libraries(krbd ${KEYUTILS_LIBRARIES} ${UDEV_LIBRARIES})
  endif()
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
endif(WITH_RBD)

# RadosGW
if(WITH_KVS)
  add_subdirectory(key_value_store)
endif(WITH_KVS)

if(WITH_RADOSGW)
  set(civetweb_common_files civetweb/src/civetweb.c)
  add_library(civetweb_common_objs OBJECT ${civetweb_common_files})
  target_include_directories(civetweb_common_objs SYSTEM PRIVATE
	"${CMAKE_SOURCE_DIR}/src/civetweb/include")
  set_property(TARGET civetweb_common_objs
    APPEND PROPERTY COMPILE_DEFINITIONS USE_IPV6=1)
  if (LIBSSL_SONAME)
    set_property(TARGET civetweb_common_objs
      APPEND PROPERTY COMPILE_DEFINITIONS SSL_LIB="${LIBSSL_SONAME}")
    set_property(TARGET civetweb_common_objs
      APPEND PROPERTY COMPILE_DEFINITIONS CRYPTO_LIB="${LIBCRYPTO_SONAME}")
  endif()

  add_subdirectory(rgw)

endif(WITH_RADOSGW)

install(FILES
  sample.ceph.conf
  DESTINATION ${CMAKE_INSTALL_DOCDIR})

# Now create a useable config.h
configure_file(
  ${CMAKE_SOURCE_DIR}/src/include/config-h.in.cmake
  ${CMAKE_BINARY_DIR}/include/acconfig.h
)

add_subdirectory(brag)

# Everything you need to spin up a cluster with vstart.sh
add_custom_target(vstart-base DEPENDS
    ceph-osd
    ceph-mon
    ceph-mgr
    ceph-authtool
    ceph-conf
    monmaptool
    crushtool
    rados
    cython${PY_BINDING_INFIX}_rados)

add_custom_target(vstart DEPENDS
    vstart-base
    ceph-mds
    cython${PY_BINDING_INFIX}_rbd)
if(WITH_RADOSGW)
  add_dependencies(vstart radosgw radosgw-admin)
endif(WITH_RADOSGW)

if(WITH_LTTNG)
  add_dependencies(vstart tracepoint_libraries)
endif(WITH_LTTNG)

# Everything you need to run CephFS tests
add_custom_target(cephfs_testing DEPENDS
    vstart
    rados
    cython${PY_BINDING_INFIX}_modules
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

if(WITH_EMBEDDED)
  add_subdirectory(libcephd)
endif()
