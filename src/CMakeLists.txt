include(GetGitRevisionDescription)

enable_language(C ASM)
include(GNUInstallDirs)
# for erasure and compressor plugins
set(CMAKE_INSTALL_PKGLIBDIR ${CMAKE_INSTALL_LIBDIR}/${PROJECT_NAME})
# to be compatible with configure_files shared with autoconfig
set(bindir ${CMAKE_INSTALL_BINDIR})
set(sbindir ${CMAKE_INSTALL_SBINDIR})
set(libdir ${CMAKE_INSTALL_LIBDIR})
set(sysconfdir ${CMAKE_INSTALL_SYSCONFDIR})
set(pkgdatadir ${CMAKE_INSTALL_DATADIR})
set(datadir ${CMAKE_INSTALL_DATADIR}/${PROJECT_NAME})
set(prefix ${CMAKE_INSTALL_PREFIX})

add_definitions("-DCEPH_LIBDIR=\"${CMAKE_INSTALL_LIBDIR}\"")
add_definitions("-DCEPH_PKGLIBDIR=\"${CMAKE_INSTALL_PKGLIBDIR}\"")
add_definitions("-DHAVE_CONFIG_H -D__CEPH__ -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE")

set(CMAKE_ASM_COMPILER  ${PROJECT_SOURCE_DIR}/src/yasm-wrapper)
set(CMAKE_ASM_FLAGS "-f elf64")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -rdynamic -Wall -Wtype-limits -Wignored-qualifiers -Winit-self -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char -fPIC")

include(CheckCCompilerFlag)
CHECK_C_COMPILER_FLAG("-Wp,-D_FORTIFY_SOURCE=2" HAS_FORTIFY_SOURCE)
if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
if(HAS_FORTIFY_SOURCE)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wp,-D_FORTIFY_SOURCE=2")
endif()
endif()
CHECK_C_COMPILER_FLAG(-fstack-protector-strong HAS_STACK_PROTECT)
if (HAS_STACK_PROTECT)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fstack-protector-strong")
endif()

execute_process(
  COMMAND yasm -f elf64 ${CMAKE_SOURCE_DIR}/src/common/crc32c_intel_fast_asm.S -o /dev/null
  RESULT_VARIABLE no_yasm
  OUTPUT_QUIET)
if(no_yasm)
  message(STATUS " we do not have a modern/working yasm")
else(no_yasm)
  message(STATUS " we have a modern and working yasm")
  execute_process(
    COMMAND uname -m
    OUTPUT_VARIABLE arch
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(arch STREQUAL "x86_64")
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
  else(arch STREQUAL "x86_64")
    message(STATUS " we are not x86_64 && !x32")
  endif(arch STREQUAL "x86_64")
endif(no_yasm)


set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -ftemplate-depth-1024 -Wno-invalid-offsetof -Wnon-virtual-dtor -Wno-invalid-offsetof -Wstrict-null-sentinel -Woverloaded-virtual")

# require c++11
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
if (COMPILER_SUPPORTS_CXX11)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
  message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support.")
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


## detect sse support

# create a tmp file with an empty main()
set(sse_srcs "${CMAKE_BINARY_DIR}/src/erasure-code/jerasure/tmp_sse.c")
file(WRITE ${sse_srcs} "void main() {}")

# try each -msse flag
try_compile(INTEL_SSE ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-msse")
try_compile(INTEL_SSE2 ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-msse2")
try_compile(INTEL_SSE3 ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-msse3")
try_compile(INTEL_SSSE3 ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-mssse3")
try_compile(INTEL_SSE4_1 ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-msse4.1")
try_compile(INTEL_SSE4_2 ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-msse4.2")
try_compile(ARM_NEON ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-mfpu=neon")
try_compile(ARM_NEON2 ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-march=armv8-a+simd")
try_compile(ARM_CRC ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-march=armv8-a+crc")

# clean up tmp file
file(REMOVE ${sse_srcs})

if(ARM_CRC)
  set(HAVE_ARMV8_CRC 1)
  set(ARM_CRC_FLAGS "-march=armv8-a+crc -DARCH_AARCH64")
endif(ARM_CRC)

if(ARM_NEON OR ARM_NEON2)
  set(HAVE_NEON 1)
  if(ARM_NEON)
    set(ARM_NEON_FLAGS "-mfpu=neon -DARM_NEON")
  else(ARM_NEON)
    set(ARM_NEON_FLAGS "-march=armv8-a+simd -DARCH_AARCH64 -DARM_NEON")
  endif(ARM_NEON)
else(ARM_NEON OR ARM_NEON2)
  message(STATUS "Skipping target ec_jerasure_neon & ec_shec_neon: Architecture not ARM")
endif(ARM_NEON OR ARM_NEON2)

if(INTEL_SSE)
  set(HAVE_SSE 1)
  set(SSE3_FLAGS "-msse")
  if (INTEL_SSE2)
    set(HAVE_SSE2 1)
    set(SSE3_FLAGS "${SSE3_FLAGS} -msse2")
  endif (INTEL_SSE2)
  if (INTEL_SSE3)
    set(SSE3_FLAGS "${SSE3_FLAGS} -msse3")
  endif (INTEL_SSE3)
  if (INTEL_SSSE3)
    set(SSE3_FLAGS "${SSE3_FLAGS} -mssse3")
  endif (INTEL_SSSE3)
else(INTEL_SSE)
  message(STATUS "Skipping target ec_jerasure_sse3 & ec_shec_sse3: -msse not supported")
endif(INTEL_SSE)

if(INTEL_SSE4_1)
  set(SSE4_FLAGS "${SSE3_FLAGS} -msse4.1")
  if (INTEL_SSE4_2)
    set(SSE4_FLAGS "${SSE4_FLAGS} -msse4.2")
  endif (INTEL_SSE4_2)
else(INTEL_SSE4_1)
  message(STATUS "Skipping target ec_jerasure_sse4 & ec_shec_sse4: -msse4.1 not supported")
endif(INTEL_SSE4_1)


set(EXTRALIBS uuid rt dl ${ATOMIC_OPS_LIBRARIES})

if(${WITH_PROFILER})
  list(APPEND EXTRALIBS profiler)
endif(${WITH_PROFILER})

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
find_package(PythonInterp 2 QUIET)
if(NOT PYTHONINTERP_FOUND)
  message(FATAL_ERROR "Python 2 interpreter not found.")
endif(NOT PYTHONINTERP_FOUND)
find_package(PythonLibs REQUIRED)

# if CMAKE_INSTALL_PREFIX is an empty string, must replace
# it with "/" to make PYTHON_INSTALL_TEMPLATE an absolute path to be
# consistent with all other installation paths.
if(CMAKE_INSTALL_PREFIX)
  set(PYTHON_INSTALL_TEMPLATE "${CMAKE_INSTALL_PREFIX}")
else(CMAKE_INSTALL_PREFIX)
  set(PYTHON_INSTALL_TEMPLATE "/")
endif(CMAKE_INSTALL_PREFIX)

execute_process(
  COMMAND
  ${PYTHON_EXECUTABLE} -c "from distutils import sysconfig; print sysconfig.get_python_lib(1,0,prefix='${PYTHON_INSTALL_TEMPLATE}')"
  OUTPUT_VARIABLE PYTHON_INSTDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE)

if(HAVE_XIO)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${XIO_INCLUDE_DIR}")
  list(APPEND EXTRALIBS ${XIO_LIBRARY} pthread rt)
endif(HAVE_XIO)

# sort out which allocator to use
if(ALLOCATOR STREQUAL "tcmalloc")
  set(ALLOC_LIBS tcmalloc)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  set(TCMALLOC_srcs perfglue/heap_profiler.cc)
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
  mds/mdstypes.cc)

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

if(HAVE_GOOD_YASM_ELF64)
  set(yasm_srcs
    common/crc32c_intel_fast_asm.S
    common/crc32c_intel_fast_zero_asm.S)
endif(HAVE_GOOD_YASM_ELF64)

add_library(common_texttable_obj OBJECT
  common/TextTable.cc)

set(libcommon_files
  ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h
  ceph_ver.c
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
  $<TARGET_OBJECTS:common_texttable_obj>
  log/Log.cc
  log/SubsystemMap.cc
  mon/MonCap.cc
  mon/MonClient.cc
  mon/MonMap.cc
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
  msg/async/EventEpoll.cc
  msg/async/EventSelect.cc
  msg/async/net_handler.cc
  ${xio_common_srcs}
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
  common/buffer.cc
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
  osdc/Striper.cc
  osdc/Objecter.cc
  common/Graylog.cc
  common/fs_types.cc
  common/dns_resolve.cc
  ${arch_files}
  ${auth_files}
  $<TARGET_OBJECTS:compressor_objs>
  ${mds_files})
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

if(${HAVE_GPERFTOOLS})
  list(APPEND libcommon_files
    perfglue/cpu_profiler.cc)
else()
  list(APPEND libcommon_files
    perfglue/disabled_stubs.cc)
endif(${HAVE_GPERFTOOLS})

if(ENABLE_SHARED)
  list(APPEND libcommon_files
    $<TARGET_OBJECTS:global_common_objs>)
endif(ENABLE_SHARED)

add_library(common STATIC ${libcommon_files}
  $<TARGET_OBJECTS:mon_common_objs>
  $<TARGET_OBJECTS:common_mountcephfs_objs>)
target_link_libraries(common ${CMAKE_DL_LIBS})

set_source_files_properties(${CMAKE_SOURCE_DIR}/src/ceph_ver.c
  ${CMAKE_SOURCE_DIR}/src/common/version.cc
  ${CMAKE_SOURCE_DIR}/src/test/encoding/ceph_dencoder.cc
  APPEND PROPERTY OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h)

if(${HAVE_GPERFTOOLS})
  target_link_libraries(common profiler)
endif(${HAVE_GPERFTOOLS})

if(${HAVE_ARMV8_CRC})
  add_library(common_crc_aarch64 STATIC common/crc32c_aarch64.c)
  set_target_properties(common_crc_aarch64 PROPERTIES COMPILE_FLAGS "${CMAKE_C_FLAGS} ${ARM_CRC_FLAGS}")
  target_link_libraries(common common_crc_aarch64)
endif(${HAVE_ARMV8_CRC})

add_library(common_utf8 STATIC common/utf8.c)

target_link_libraries(common json_spirit common_utf8 erasure_code rt uuid resolv ${CRYPTO_LIBS} ${Boost_LIBRARIES} ${BLKID_LIBRARIES} ${EXECINFO_LIBRARIES})

if(${WITH_LTTNG})
  add_subdirectory(tracing)
endif(${WITH_LTTNG})

set(libglobal_srcs
  global/global_init.cc
  global/pidfile.cc
  global/signal_handler.cc)
set(global_common_files
  global/global_context.cc)
add_library(global_common_objs OBJECT ${global_common_files})
add_library(global STATIC ${libglobal_srcs}
  $<TARGET_OBJECTS:global_common_objs>)
target_link_libraries(global common ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS}
  ${EXTRALIBS})
if(${WITH_LTTNG})
  target_link_libraries(global lttng-ust dl)
endif(${WITH_LTTNG})

# rados object classes
add_subdirectory(cls)

# RADOS client/library
set(osdc_files
  osdc/Objecter.cc
  osdc/Filer.cc)
set(osdc_rbd_files
  osdc/ObjectCacher.cc
  osdc/Striper.cc)
add_library(osdc_rbd_objs OBJECT ${osdc_rbd_files})
add_library(osdc STATIC ${osdc_files} $<TARGET_OBJECTS:osdc_rbd_objs>)

set(librados_srcs
  librados/librados.cc
  librados/RadosClient.cc
  librados/IoCtxImpl.cc
  librados/snap_set_diff.cc
  librados/RadosXattrIter.cc
  )
add_library(librados ${CEPH_SHARED} ${librados_srcs}
  $<TARGET_OBJECTS:cls_references_objs>
  $<TARGET_OBJECTS:common_util_obj>)
add_dependencies(librados osdc)
if(WITH_LTTNG)
  add_dependencies(librados rados-tp)
endif()
# LINK_PRIVATE instead of PRIVATE is used to backward compatibility with cmake 2.8.11
target_link_libraries(librados LINK_PRIVATE osdc osd os global common cls_lock_client
  ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS})
if(ENABLE_SHARED)
  set_target_properties(librados PROPERTIES OUTPUT_NAME rados VERSION 2.0.0
    SOVERSION 2)
endif(ENABLE_SHARED)

add_library(librados_api STATIC common/buffer.cc librados/librados.cc)

install(FILES include/rados/librados.h
  include/rados/rados_types.h
  include/rados/rados_types.hpp
  include/rados/librados.hpp
  include/buffer.h
  include/buffer_fwd.h
  include/memory.h
  include/page.h
  include/crc32c.h
  DESTINATION include/rados)
install(TARGETS librados DESTINATION lib)

add_subdirectory(libradosstriper)
install(FILES
  include/radosstriper/libradosstriper.h
  include/radosstriper/libradosstriper.hpp
  DESTINATION include/radosstriper)

add_executable(ceph_scratchtool tools/scratchtool.c)
target_link_libraries(ceph_scratchtool librados global)
install(TARGETS ceph_scratchtool DESTINATION bin)

add_executable(ceph_scratchtoolpp tools/scratchtoolpp.cc)
target_link_libraries(ceph_scratchtoolpp librados global)
install(TARGETS ceph_scratchtoolpp DESTINATION bin)

add_executable(ceph_radosacl tools/radosacl.cc)
target_link_libraries(ceph_radosacl librados global)
install(TARGETS ceph_radosacl DESTINATION bin)

add_executable(ceph-osdomap-tool tools/ceph_osdomap_tool.cc)
target_link_libraries(ceph-osdomap-tool os global ${Boost_PROGRAM_OPTIONS_LIBRARY})
install(TARGETS ceph-osdomap-tool DESTINATION bin)

add_executable(ceph-monstore-tool tools/ceph_monstore_tool.cc)
target_link_libraries(ceph-monstore-tool os global ${Boost_PROGRAM_OPTIONS_LIBRARY})
install(TARGETS ceph-monstore-tool DESTINATION bin)
install(PROGRAMS
  tools/ceph-monstore-update-crush.sh
  DESTINATION lib/ceph)

add_executable(ceph-objectstore-tool
  tools/ceph_objectstore_tool.cc
  tools/RadosDump.cc
  $<TARGET_OBJECTS:common_util_obj>)
target_link_libraries(ceph-objectstore-tool tcmalloc osd os global ${Boost_PROGRAM_OPTIONS_LIBRARY} fuse dl)
install(TARGETS ceph-objectstore-tool DESTINATION bin)


set(rados_srcs
  tools/rados/rados.cc
  tools/RadosDump.cc
  tools/rados/RadosImport.cc
  tools/rados/PoolDump.cc
  common/obj_bencher.cc)
add_executable(rados ${rados_srcs})
target_link_libraries(rados librados global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} radosstriper)

if (WITH_CEPHFS)
  set(cephfs_journal_tool_srcs
    tools/cephfs/cephfs-journal-tool.cc
    tools/cephfs/JournalTool.cc
    tools/cephfs/JournalFilter.cc
    tools/cephfs/JournalScanner.cc
    tools/cephfs/EventOutput.cc
    tools/cephfs/Dumper.cc
    tools/cephfs/Resetter.cc
    tools/cephfs/RoleSelector.cc
    tools/cephfs/MDSUtility.cc)
  add_executable(cephfs-journal-tool ${cephfs_journal_tool_srcs})
  target_link_libraries(cephfs-journal-tool librados mds osdc global
                        ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

  set(cephfs_table_tool_srcs
    tools/cephfs/cephfs-table-tool.cc
    tools/cephfs/TableTool.cc
    tools/cephfs/RoleSelector.cc
    tools/cephfs/MDSUtility.cc)
  add_executable(cephfs-table-tool ${cephfs_table_tool_srcs})
  target_link_libraries(cephfs-table-tool librados mds osdc global
                        ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

  set(cephfs_data_scan_srcs
    tools/cephfs/cephfs-data-scan.cc
    tools/cephfs/DataScan.cc
    tools/cephfs/RoleSelector.cc
    tools/cephfs/MDSUtility.cc)
  add_executable(cephfs-data-scan ${cephfs_data_scan_srcs})
  target_link_libraries(cephfs-data-scan librados mds osdc global
    cls_cephfs_client
    ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

  install(TARGETS
    cephfs-journal-tool
    cephfs-table-tool
    cephfs-data-scan
    DESTINATION bin)
endif (WITH_CEPHFS)

set(librados_config_srcs
  librados-config.cc)
add_executable(librados-config ${librados_config_srcs})
target_link_libraries(librados-config librados global ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS})

install(TARGETS rados librados-config DESTINATION bin)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/
  DESTINATION ${PYTHON_INSTDIR})

# virtualenv base directory for ceph-disk and ceph-detect-init
set(CEPH_BUILD_VIRTUALENV $ENV{TMPDIR})
if(NOT CEPH_BUILD_VIRTUALENV)
  set(CEPH_BUILD_VIRTUALENV /tmp)
endif()

add_subdirectory(pybind)
add_subdirectory(ceph-disk)
add_subdirectory(ceph-detect-init)

## dencoder
set(dencoder_srcs
  test/encoding/ceph_dencoder.cc
  $<TARGET_OBJECTS:krbd_objs>
  $<TARGET_OBJECTS:parse_secret_objs>
  $<TARGET_OBJECTS:common_texttable_obj>
  )
if(${WITH_RADOSGW})
  list(APPEND dencoder_srcs
    rgw/rgw_dencoder.cc
  )
  set(DENCODER_EXTRALIBS
    rgw_a
    cls_version_client
    cls_log_client
    cls_refcount_client
    cls_user_client
    cls_statelog_client
    cls_rgw_client
    curl
    expat
    fcgi
  )
endif(${WITH_RADOSGW})
if(${WITH_RBD})
  set(DENCODER_EXTRALIBS
      ${DENCODER_EXTRALIBS}
      rbd_replay_types)
endif(${WITH_RBD})

add_executable(ceph-dencoder ${dencoder_srcs})
target_link_libraries(ceph-dencoder
  librados
  librbd
  global
  os
  osd
  mds
  mon
  osdc
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
  blkid
  udev
  keyutils
  rbd_replay
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS ceph-dencoder DESTINATION bin)

# Monitor
set(lib_mon_srcs
  auth/cephx/CephxKeyServer.cc
  auth/cephx/CephxServiceHandler.cc
  auth/AuthServiceHandler.cc
  ${osd_mon_files} mon/Paxos.cc
  mon/PaxosService.cc
  mon/OSDMonitor.cc
  mon/MDSMonitor.cc
  mon/MonmapMonitor.cc
  mon/LogMonitor.cc
  mon/AuthMonitor.cc
  mon/Elector.cc
  mon/HealthMonitor.cc
  ${os_mon_files}
  mon/DataHealthService.cc
  mon/PGMonitor.cc
  mon/PGMap.cc
  mon/ConfigKeyService.cc)

set(common_util_src
  common/util.cc)
add_library(common_util_obj OBJECT ${common_util_src})
add_library(mon STATIC ${lib_mon_srcs} $<TARGET_OBJECTS:mon_common_objs>
  $<TARGET_OBJECTS:kv_objs> $<TARGET_OBJECTS:osd_mon_objs>
  $<TARGET_OBJECTS:common_util_obj> $<TARGET_OBJECTS:heap_profiler_objs>)
target_link_libraries(mon ${ALLOC_LIBS})

set(ceph_mon_srcs
  ceph_mon.cc)
add_executable(ceph-mon ${ceph_mon_srcs}
  $<TARGET_OBJECTS:common_texttable_obj>)
add_dependencies(ceph-mon erasure_code_plugins)
      target_link_libraries(ceph-mon mon common os global ${EXTRALIBS}
  ${CMAKE_DL_LIBS})
install(TARGETS ceph-mon DESTINATION bin)

# OSD
if(${HAVE_LIBXFS})
  set(libos_xfs_srcs
    os/filestore/XfsFileStoreBackend.cc
    os/fs/XFS.cc)
endif(${HAVE_LIBXFS})
set(libos_srcs
  os/ObjectStore.cc
  os/Transaction.cc
  os/filestore/chain_xattr.cc
  os/filestore/BtrfsFileStoreBackend.cc
  os/filestore/DBObjectMap.cc
  os/filestore/FileJournal.cc
  os/filestore/FileStore.cc
  os/filestore/JournalThrottle.cc
  os/filestore/GenericFileStoreBackend.cc
  os/filestore/JournalingObjectStore.cc
  os/filestore/HashIndex.cc
  os/filestore/IndexManager.cc
  os/filestore/LFNIndex.cc
  os/filestore/WBThrottle.cc
  os/filestore/ZFSFileStoreBackend.cc
  os/memstore/MemStore.cc
  os/kstore/KStore.cc
  os/kstore/kstore_types.cc
  os/bluestore/kv.cc
  os/bluestore/Allocator.cc
  os/bluestore/BitmapFreelistManager.cc
  os/bluestore/BlockDevice.cc
  os/bluestore/BlueFS.cc
  os/bluestore/bluefs_types.cc
  os/bluestore/BlueRocksEnv.cc
  os/bluestore/BlueStore.cc
  os/bluestore/bluestore_types.cc
  os/bluestore/ExtentFreelistManager.cc
  os/bluestore/FreelistManager.cc
  os/bluestore/KernelDevice.cc
  os/bluestore/StupidAllocator.cc
  os/bluestore/BitMapAllocator.cc
  os/bluestore/BitAllocator.cc
  os/fs/FS.cc
  ${libos_xfs_srcs})
if(${HAVE_LIBFUSE})
  list(APPEND libos_srcs
    os/FuseStore.cc)
endif(${HAVE_LIBFUSE})
if(WITH_SPDK)
  list(APPEND libos_srcs
    os/bluestore/NVMEDevice.cc)
endif(WITH_SPDK)

if(WITH_SPDK)
  add_custom_target(build_spdk
    COMMAND
    $(MAKE) DPDK_INC_DIR=${DPDK_INCLUDE_DIR}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/spdk/lib
    COMMENT "spdk building")
  # TODO: should use add_library(spdk INTERFACE IMPORTED) instead in new cmake,
  # if INTERFACE is supported.
  foreach(lib nvme memory util)
    add_library(spdk_${lib} STATIC IMPORTED)
    add_dependencies(spdk_${lib} build_spdk)
    set_target_properties(spdk_${lib} PROPERTIES
      IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/src/spdk/lib/${lib}/libspdk_${lib}.a"
      INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src/spdk/include")
    list(APPEND SPDK_LIBRARIES spdk_${lib})
  endforeach()
endif(WITH_SPDK)

# make rocksdb statically
if(NOT ALLOCATOR STREQUAL "jemalloc")
  set(disable_jemalloc "DISABLE_JEMALLOC=1")
endif()
add_custom_target(build_rocksdb
    COMMAND
    PORTABLE=1 ${disable_jemalloc} $(MAKE) static_lib EXTRA_CXXFLAGS='-fPIC -Wno-unused-variable'
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/rocksdb
    COMMENT "rocksdb building")

# add a imported library for librocksdb.a
add_library(rocksdb STATIC IMPORTED)
set_property(TARGET rocksdb PROPERTY IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/src/rocksdb/librocksdb.a")
add_dependencies(rocksdb build_rocksdb)
set(ROCKSDB_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/rocksdb/include)

add_subdirectory(kv)

add_library(os STATIC ${libos_srcs} $<TARGET_OBJECTS:kv_objs>)
if(${HAVE_LIBAIO})
  target_link_libraries(os aio)
endif(${HAVE_LIBAIO})
if(${HAVE_LIBFUSE})
  target_link_libraries(os ${FUSE_LIBRARIES})
endif(${HAVE_LIBFUSE})
if(WITH_SPDK)
  target_link_libraries(os
    ${SPDK_LIBRARIES}
    ${DPDK_LIBRARIES}
    ${PCIACCESS_LIBRARIES})
  target_include_directories(os
    PRIVATE
    ${DPDK_INCLUDE_DIR}
    ${PCIACCESS_INCLUDE_DIR})
endif(WITH_SPDK)
if(WITH_LTTNG)
  add_dependencies(os os-tp)
endif()
target_link_libraries(os kv)
if(HAVE_LIBAIO)
  add_executable(ceph-bluefs-tool
    os/bluestore/bluefs_tool.cc)
  target_link_libraries(ceph-bluefs-tool
    os global)
  install(TARGETS ceph-bluefs-tool
    DESTINATION bin)
endif()

set(cls_references_files objclass/class_api.cc)
add_library(cls_references_objs OBJECT ${cls_references_files})

set(osdc_osd_srcs
  osdc/Objecter.cc
  osdc/Striper.cc)

set(osd_srcs
  osd/OSD.cc
  osd/Watch.cc
  osd/ClassHandler.cc
  osd/OpRequest.cc
  osd/PG.cc
  osd/PGLog.cc
  osd/ReplicatedPG.cc
  osd/ReplicatedBackend.cc
  osd/ECBackend.cc
  osd/ECTransaction.cc
  osd/PGBackend.cc
  osd/OSDCap.cc
  osd/Watch.cc
  osd/ClassHandler.cc
  common/TrackedOp.cc
  osd/SnapMapper.cc
  osd/ScrubStore.cc
  osd/osd_types.cc
  osd/ECUtil.cc
  ${osdc_osd_srcs})
set(osd_mon_files
  mon/Monitor.cc)
add_library(osd_mon_objs OBJECT ${osd_mon_files})
add_library(osd STATIC ${osd_srcs}
  $<TARGET_OBJECTS:osd_mon_objs>
  $<TARGET_OBJECTS:cls_references_objs>
  $<TARGET_OBJECTS:global_common_objs>
  $<TARGET_OBJECTS:heap_profiler_objs>
  $<TARGET_OBJECTS:common_util_obj>)
target_link_libraries(osd dl leveldb ${ALLOC_LIBS})
if(WITH_LTTNG)
  add_dependencies(osd osd-tp)
endif()

set(ceph_osd_srcs
  ceph_osd.cc)
add_executable(ceph-osd ${ceph_osd_srcs}
  $<TARGET_OBJECTS:common_util_obj>)
add_dependencies(ceph-osd erasure_code_plugins)
target_link_libraries(ceph-osd osd os global ${BLKID_LIBRARIES})
if(${HAVE_LIBFUSE})
  target_link_libraries(ceph-osd ${FUSE_LIBRARIES})
endif(${HAVE_LIBFUSE})
install(TARGETS ceph-osd DESTINATION bin)

# MDS
if(${WITH_MDS})
  set(mds_srcs
    mds/Capability.cc
    mds/MDSDaemon.cc
    mds/MDSRank.cc
    mds/Beacon.cc
    mds/flock.cc
    mds/locks.c
    mds/journal.cc
    mds/Server.cc
    mds/Mutation.cc
    mds/MDCache.cc
    mds/RecoveryQueue.cc
    mds/StrayManager.cc
    mds/Locker.cc
    mds/Migrator.cc
    mds/MDBalancer.cc
    mds/CDentry.cc
    mds/CDir.cc
    mds/CInode.cc
    mds/LogEvent.cc
    mds/MDSTable.cc
    mds/InoTable.cc
    mds/JournalPointer.cc
    mds/MDSTableClient.cc
    mds/MDSTableServer.cc
    mds/ScrubStack.cc
    mds/DamageTable.cc
    mds/SimpleLock.cc
    mds/SnapRealm.cc
    mds/SnapServer.cc
    mds/snap.cc
    mds/SessionMap.cc
    mds/MDSContext.cc
    mds/MDSAuthCaps.cc
    mds/MDLog.cc
    ${CMAKE_SOURCE_DIR}/src/common/TrackedOp.cc
    ${CMAKE_SOURCE_DIR}/src/osdc/Journaler.cc)
  add_library(mds STATIC ${mds_srcs}
    $<TARGET_OBJECTS:heap_profiler_objs>
    $<TARGET_OBJECTS:common_util_obj>)
  target_link_libraries(mds ${ALLOC_LIBS} osdc common)
  set(ceph_mds_srcs
    ceph_mds.cc)
  add_executable(ceph-mds ${ceph_mds_srcs}
    $<TARGET_OBJECTS:common_util_obj>)
  target_link_libraries(ceph-mds mds ${CMAKE_DL_LIBS} global
    ${Boost_THREAD_LIBRARY})
  install(TARGETS ceph-mds DESTINATION bin)
endif(${WITH_MDS})

add_subdirectory(erasure-code)

set(crushtool_srcs
    tools/crushtool.cc)
add_executable(crushtool ${crushtool_srcs})
target_link_libraries(crushtool global)
install(TARGETS crushtool DESTINATION bin)

# Support/Tools
add_subdirectory(gmock)

add_subdirectory(test)
set(cephfs_srcs cephfs.cc)
add_executable(cephfstool ${cephfs_srcs})
target_link_libraries(cephfstool common ${EXTRALIBS})
set_target_properties(cephfstool PROPERTIES OUTPUT_NAME cephfs)
install(TARGETS cephfstool DESTINATION bin)

add_subdirectory(compressor)

add_executable(ceph-client-debug tools/ceph-client-debug.cc)
target_link_libraries(ceph-client-debug cephfs librados global common)
install(TARGETS ceph-client-debug DESTINATION bin)

add_executable(ceph-kvstore-tool tools/ceph_kvstore_tool.cc)
target_link_libraries(ceph-kvstore-tool os global ${UNITTEST_CXX_FLAGS})
install(TARGETS ceph-kvstore-tool DESTINATION bin)

set(ceph_conf_srcs
  tools/ceph_conf.cc)
add_executable(ceph-conf ${ceph_conf_srcs})
target_link_libraries(ceph-conf global)
install(TARGETS ceph-conf DESTINATION bin)

set(monmaptool_srcs
  tools/monmaptool.cc)
add_executable(monmaptool ${monmaptool_srcs})
target_link_libraries(monmaptool global)
install(TARGETS monmaptool DESTINATION bin)

set(osdomaptool_srcs
  tools/osdmaptool.cc)
add_executable(osdmaptool ${osdomaptool_srcs})
target_link_libraries(osdmaptool global)
install(TARGETS osdmaptool DESTINATION bin)

set(ceph_psim_srcs
  tools/psim.cc)
add_executable(ceph_psim ${ceph_psim_srcs})
target_link_libraries(ceph_psim global)
install(TARGETS ceph_psim DESTINATION bin)

set(ceph_authtool_srcs
  tools/ceph_authtool.cc)
add_executable(ceph-authtool ${ceph_authtool_srcs})
target_link_libraries(ceph-authtool global ${EXTRALIBS} ${CRYPTO_LIBS})
install(TARGETS ceph-authtool DESTINATION bin)

configure_file(${CMAKE_SOURCE_DIR}/src/ceph-coverage.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-coverage @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/ceph-debugpack.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-debugpack @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/ceph.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/ceph-crush-location.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-crush-location @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/init-ceph.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/init-ceph @ONLY)

configure_file(ceph-post-file.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-post-file @ONLY)

install(PROGRAMS
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-debugpack
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-coverage
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-crush-location
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-post-file
  ${CMAKE_SOURCE_DIR}/src/ceph-run
  ${CMAKE_SOURCE_DIR}/src/ceph-rest-api
  ${CMAKE_SOURCE_DIR}/src/vstart.sh
  ${CMAKE_SOURCE_DIR}/src/ceph-clsinfo
  DESTINATION bin)
install(PROGRAMS
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/init-ceph
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/init.d
  RENAME ceph)

install(FILES
  ${CMAKE_SOURCE_DIR}/share/id_dsa_drop.ceph.com
  ${CMAKE_SOURCE_DIR}/share/id_dsa_drop.ceph.com.pub
  ${CMAKE_SOURCE_DIR}/share/known_hosts_drop.ceph.com
  DESTINATION ${CMAKE_INSTALL_DATADIR}/ceph)

install(FILES
  ${CMAKE_SOURCE_DIR}/doc/start/ceph.conf
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/ceph/ RENAME ceph.conf.example)

install(PROGRAMS
  ceph_common.sh
  ceph-osd-prestart.sh
  DESTINATION ${CMAKE_INSTALL_PREFIX}/lib/ceph)

install(PROGRAMS
  ${CMAKE_SOURCE_DIR}/src/ceph-create-keys
#  ${CMAKE_SOURCE_DIR}/src/ceph-disk
  ${CMAKE_SOURCE_DIR}/src/ceph-disk-udev
  DESTINATION sbin)

add_subdirectory(bash_completion)

set(parse_secret_files
  common/secret.c)
add_library(parse_secret_objs OBJECT ${parse_secret_files})

if(WITH_LIBCEPHFS)
  set(libclient_srcs
    client/Client.cc
    client/Dentry.cc
    client/Inode.cc
    client/MetaRequest.cc
    client/ClientSnapRealm.cc
    client/MetaSession.cc
    client/Trace.cc
    client/posix_acl.cc)
  add_library(client STATIC ${libclient_srcs})
  target_link_libraries(client osdc mds)
  set(libcephfs_srcs libcephfs.cc)
  add_library(cephfs ${CEPH_SHARED} ${libcephfs_srcs}
    $<TARGET_OBJECTS:cls_references_objs>
    $<TARGET_OBJECTS:common_util_obj>)
  target_link_libraries(cephfs LINK_PRIVATE client osdc osd os global common
    ${BLKID_LIBRARIES}
    ${CRYPTO_LIBS} ${EXTRALIBS})
if(ENABLE_SHARED)
  set_target_properties(cephfs PROPERTIES OUTPUT_NAME cephfs VERSION 1.0.0
    SOVERSION 1)
endif(ENABLE_SHARED)
  install(TARGETS cephfs DESTINATION lib)
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

  if(HAVE_LIBFUSE)
    set(ceph_fuse_srcs
      ceph_fuse.cc
      client/fuse_ll.cc)
    add_executable(ceph-fuse ${ceph_fuse_srcs})
    target_link_libraries(ceph-fuse ${FUSE_LIBRARIES} client global)
    set_target_properties(ceph-fuse PROPERTIES COMPILE_FLAGS "-I${FUSE_INCLUDE_DIRS}")
    install(TARGETS ceph-fuse DESTINATION bin)
    install(PROGRAMS mount.fuse.ceph DESTINATION ${CMAKE_INSTALL_SBINDIR})
  endif(HAVE_LIBFUSE)
endif(WITH_LIBCEPHFS)

set(journal_srcs
  journal/AsyncOpTracker.cc
  journal/Entry.cc
  journal/Future.cc
  journal/FutureImpl.cc
  journal/Journaler.cc
  journal/JournalMetadata.cc
  journal/JournalPlayer.cc
  journal/JournalRecorder.cc
  journal/JournalTrimmer.cc
  journal/ObjectPlayer.cc
  journal/ObjectRecorder.cc
  journal/Utils.cc)
add_library(journal STATIC ${journal_srcs})

add_library(krbd_objs OBJECT krbd.cc)

if(${WITH_RBD})
  add_subdirectory(librbd)
  install(FILES
    include/rbd/features.h
    include/rbd/librbd.h
    include/rbd/librbd.hpp
    DESTINATION include/rbd)

  add_executable(rbd-nbd tools/rbd_nbd/rbd-nbd.cc
    $<TARGET_OBJECTS:parse_secret_objs>)
  target_link_libraries(rbd-nbd librbd librados global keyutils
    ${Boost_REGEX_LIBRARY})

  set(rbd_mirror_internal
    tools/rbd_mirror/ClusterWatcher.cc
    tools/rbd_mirror/ImageReplayer.cc
    tools/rbd_mirror/ImageDeleter.cc
    tools/rbd_mirror/ImageSync.cc
    tools/rbd_mirror/Mirror.cc
    tools/rbd_mirror/PoolWatcher.cc
    tools/rbd_mirror/Replayer.cc
    tools/rbd_mirror/Threads.cc
    tools/rbd_mirror/types.cc
    tools/rbd_mirror/image_replayer/BootstrapRequest.cc
    tools/rbd_mirror/image_replayer/CloseImageRequest.cc
    tools/rbd_mirror/image_replayer/CreateImageRequest.cc
    tools/rbd_mirror/image_replayer/OpenImageRequest.cc
    tools/rbd_mirror/image_replayer/OpenLocalImageRequest.cc
    tools/rbd_mirror/image_replayer/ReplayStatusFormatter.cc
    tools/rbd_mirror/image_sync/ImageCopyRequest.cc
    tools/rbd_mirror/image_sync/ObjectCopyRequest.cc
    tools/rbd_mirror/image_sync/SnapshotCopyRequest.cc
    tools/rbd_mirror/image_sync/SnapshotCreateRequest.cc
    tools/rbd_mirror/image_sync/SyncPointCreateRequest.cc
    tools/rbd_mirror/image_sync/SyncPointPruneRequest.cc)
  add_library(rbd_mirror_internal STATIC ${rbd_mirror_internal})

  if(WITH_FUSE)
    add_executable(rbd-fuse
      rbd_fuse/rbd-fuse.cc)
    target_link_libraries(rbd-fuse
      librbd librados ${FUSE_LIBRARIES})
    install(TARGETS rbd-fuse DESTINATION bin)
  endif()

  add_executable(rbd-mirror
    tools/rbd_mirror/main.cc
    common/ContextCompletion.cc)
  target_link_libraries(rbd-mirror
    rbd_mirror_internal
    rbd_internal
    rbd_api
    rbd_types
    journal
    librados
    osdc
    cls_rbd_client
    cls_lock_client
    cls_journal_client
    global)

  set(rbd_srcs
    tools/rbd/rbd.cc
    tools/rbd/ArgumentTypes.cc
    tools/rbd/IndentStream.cc
    tools/rbd/OptionPrinter.cc
    tools/rbd/Shell.cc
    tools/rbd/Utils.cc
    tools/rbd/action/BenchWrite.cc
    tools/rbd/action/Children.cc
    tools/rbd/action/Clone.cc
    tools/rbd/action/Copy.cc
    tools/rbd/action/Create.cc
    tools/rbd/action/Diff.cc
    tools/rbd/action/DiskUsage.cc
    tools/rbd/action/Export.cc
    tools/rbd/action/ExportDiff.cc
    tools/rbd/action/Feature.cc
    tools/rbd/action/Flatten.cc
    tools/rbd/action/ImageMeta.cc
    tools/rbd/action/Import.cc
    tools/rbd/action/ImportDiff.cc
    tools/rbd/action/Info.cc
    tools/rbd/action/Journal.cc
    tools/rbd/action/Kernel.cc
    tools/rbd/action/List.cc
    tools/rbd/action/Lock.cc
    tools/rbd/action/MergeDiff.cc
    tools/rbd/action/MirrorPool.cc
    tools/rbd/action/MirrorImage.cc
    tools/rbd/action/Nbd.cc
    tools/rbd/action/ObjectMap.cc
    tools/rbd/action/Remove.cc
    tools/rbd/action/Rename.cc
    tools/rbd/action/Resize.cc
    tools/rbd/action/Snap.cc
    tools/rbd/action/Status.cc
    tools/rbd/action/Watch.cc)
  add_executable(rbd ${rbd_srcs}
    $<TARGET_OBJECTS:common_util_obj>
    $<TARGET_OBJECTS:parse_secret_objs>
    $<TARGET_OBJECTS:common_texttable_obj>
    $<TARGET_OBJECTS:krbd_objs>)
  set_target_properties(rbd PROPERTIES OUTPUT_NAME rbd)
  target_link_libraries(rbd librbd librados global common keyutils udev
    ${Boost_REGEX_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY}
    ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})
  install(TARGETS rbd rbd-nbd rbd-mirror DESTINATION bin)
  install(PROGRAMS ${CMAKE_SOURCE_DIR}/src/ceph-rbdnamer DESTINATION bin)
  install(PROGRAMS ${CMAKE_SOURCE_DIR}/src/rbdmap DESTINATION bin)
  add_subdirectory(rbd_replay)
endif(${WITH_RBD})

# RadosGW
if(${WITH_KVS})
  set(kvs_srcs
    key_value_store/cls_kvs.cc)
  add_library(cls_kvs SHARED ${kvs_srcs})
  set_target_properties(cls_kvs PROPERTIES VERSION "1.0.0" SOVERSION "1")
  install(TARGETS cls_kvs DESTINATION lib/rados-classes)
endif(${WITH_KVS})

if(${WITH_RADOSGW})

  set(rgw_a_srcs
    rgw/rgw_acl.cc
    rgw/rgw_acl_s3.cc
    rgw/rgw_acl_swift.cc
    rgw/rgw_auth.cc
    rgw/rgw_auth_s3.cc
    rgw/rgw_basic_types.cc
    rgw/rgw_bucket.cc
    rgw/rgw_cache.cc
    rgw/rgw_client_io.cc
    rgw/rgw_common.cc
    rgw/rgw_cors.cc
    rgw/rgw_cors_s3.cc
    rgw/rgw_dencoder.cc
    rgw/rgw_env.cc
    rgw/rgw_fcgi.cc
    rgw/rgw_formats.cc
    rgw/rgw_frontend.cc
    rgw/rgw_gc.cc
    rgw/rgw_http_client.cc
    rgw/rgw_json_enc.cc
    rgw/rgw_keystone.cc
    rgw/rgw_ldap.cc
    rgw/rgw_loadgen.cc
    rgw/rgw_log.cc
    rgw/rgw_metadata.cc
    rgw/rgw_multi.cc
    rgw/rgw_multi_del.cc
    rgw/rgw_sync.cc
    rgw/rgw_data_sync.cc
    rgw/rgw_period_history.cc
    rgw/rgw_period_puller.cc
    rgw/rgw_period_pusher.cc
    rgw/rgw_realm_reloader.cc
    rgw/rgw_realm_watcher.cc
    rgw/rgw_coroutine.cc
    rgw/rgw_cr_rados.cc
    rgw/rgw_object_expirer_core.cc
    rgw/rgw_op.cc
    rgw/rgw_os_lib.cc
    rgw/rgw_policy_s3.cc
    rgw/rgw_process.cc
    rgw/rgw_quota.cc
    rgw/rgw_rados.cc
    rgw/rgw_replica_log.cc
    rgw/rgw_request.cc
    rgw/rgw_resolve.cc
    rgw/rgw_rest_bucket.cc
    rgw/rgw_rest.cc
    rgw/rgw_rest_client.cc
    rgw/rgw_rest_config.cc
    rgw/rgw_rest_conn.cc
    rgw/rgw_rest_log.cc
    rgw/rgw_rest_metadata.cc
    rgw/rgw_rest_opstate.cc
    rgw/rgw_rest_realm.cc
    rgw/rgw_rest_replica_log.cc
    rgw/rgw_rest_s3.cc
    rgw/rgw_rest_swift.cc
    rgw/rgw_rest_usage.cc
    rgw/rgw_rest_user.cc
    rgw/rgw_swift_auth.cc
    rgw/rgw_tools.cc
    rgw/rgw_usage.cc
    rgw/rgw_user.cc
    rgw/rgw_website.cc
    rgw/rgw_xml.cc
    rgw/rgw_xml_enc.cc
    )

  set(civetweb_common_files civetweb/src/civetweb.c)
  add_library(civetweb_common_objs OBJECT ${civetweb_common_files})
  target_include_directories(civetweb_common_objs PUBLIC
	"${CMAKE_SOURCE_DIR}/src/civetweb/include")
  if(HAVE_OPENSSL)
  set_property(TARGET civetweb_common_objs
	       APPEND PROPERTY COMPILE_DEFINITIONS NO_SSL_DL=1)
  target_include_directories(civetweb_common_objs PUBLIC
	"${OPENSSL_INCLUDE_DIR}")
  else(HAVE_OPENSSL)
  endif(HAVE_OPENSSL)

  add_library(rgw_a STATIC ${rgw_a_srcs})
  target_link_libraries(rgw_a librados cls_rgw_client cls_refcount_client
    cls_log_client cls_statelog_client cls_timeindex_client cls_version_client
    cls_replica_log_client cls_user_client curl global expat)

  set(radosgw_srcs
    rgw/rgw_fcgi_process.cc
    rgw/rgw_loadgen_process.cc
    rgw/rgw_civetweb.cc
    rgw/rgw_civetweb_frontend.cc
    rgw/rgw_civetweb_log.cc
    rgw/rgw_main.cc)

  set(radosgw_admin_srcs
    rgw/rgw_admin.cc
    rgw/rgw_orphan.cc)

  set(radosgw_token_srcs
    rgw/rgw_token.cc)

  set(radosgw_object_expirer_srcs
    rgw/rgw_object_expirer.cc)

  set(librgw_srcs
    rgw/librgw.cc
    rgw/rgw_file.cc
    )

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
    curl expat global
    resolv)
  set_target_properties(rgw PROPERTIES OUTPUT_NAME rgw VERSION 2.0.0
    SOVERSION 1)

  install(TARGETS rgw DESTINATION lib)
  install(FILES
    include/rados/librgw.h
    include/rados/rgw_file.h
  DESTINATION include/rados)

  add_executable(radosgw ${radosgw_srcs}  $<TARGET_OBJECTS:civetweb_common_objs>)
  target_link_libraries(radosgw rgw_a librados
    cls_rgw_client cls_lock_client cls_refcount_client
    cls_log_client cls_statelog_client cls_timeindex_client
    cls_version_client cls_replica_log_client cls_user_client
    curl expat global fcgi resolv ${OPENSSL_LIBRARIES} ${BLKID_LIBRARIES} ${OPENLDAP_LIBS}
    ${ALLOC_LIBS})
  install(TARGETS radosgw DESTINATION bin)

  add_executable(radosgw-admin ${radosgw_admin_srcs})
  target_link_libraries(radosgw-admin rgw_a librados
    cls_rgw_client cls_lock_client cls_refcount_client
    cls_log_client cls_statelog_client cls_timeindex_client
    cls_version_client cls_replica_log_client cls_user_client
    curl expat global fcgi resolv ${OPENSSL_LIBRARIES} ${BLKID_LIBRARIES})

  install(TARGETS radosgw-admin DESTINATION bin)

  add_executable(radosgw-token ${radosgw_token_srcs})
  target_link_libraries(radosgw-token librados
    global ${ALLOC_LIBS})
  install(TARGETS radosgw-token DESTINATION bin)

  add_executable(radosgw-object-expirer ${radosgw_object_expirer_srcs})
  target_link_libraries(radosgw-object-expirer rgw_a librados
    cls_rgw_client cls_lock_client cls_refcount_client
    cls_log_client cls_statelog_client cls_timeindex_client
    cls_version_client cls_replica_log_client cls_user_client
    curl expat global fcgi resolv)
  install(TARGETS radosgw-object-expirer DESTINATION bin)
  add_subdirectory(rgw)
endif(${WITH_RADOSGW})

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
    ceph-mds
    ceph-authtool
    ceph-conf
    monmaptool
    crushtool
    cython_rados)

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
