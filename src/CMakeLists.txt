include(GetGitRevisionDescription)
include(CheckCXXCompilerFlag)

# for erasure and compressor plugins
set(CEPH_INSTALL_PKGLIBDIR ${CMAKE_INSTALL_LIBDIR}/${PROJECT_NAME})
set(CEPH_INSTALL_FULL_PKGLIBDIR ${CMAKE_INSTALL_FULL_LIBDIR}/${PROJECT_NAME})
# for mgr plugins
set(CEPH_INSTALL_DATADIR ${CMAKE_INSTALL_FULL_DATADIR}/${PROJECT_NAME})
# so libceph-common can be found
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
if(NOT CMAKE_INSTALL_RPATH)
  set(CMAKE_INSTALL_RPATH "${CEPH_INSTALL_FULL_PKGLIBDIR}")
endif()

# to be compatible with configure_files shared with autoconfig
set(bindir ${CMAKE_INSTALL_FULL_BINDIR})
set(sbindir ${CMAKE_INSTALL_FULL_SBINDIR})
set(libdir ${CMAKE_INSTALL_FULL_LIBDIR})
set(sysconfdir ${CMAKE_INSTALL_FULL_SYSCONFDIR})
set(libexecdir ${CMAKE_INSTALL_FULL_LIBEXECDIR})
set(pkgdatadir ${CMAKE_INSTALL_FULL_DATADIR})
set(datadir ${CEPH_INSTALL_DATADIR})
set(prefix ${CMAKE_INSTALL_PREFIX})

configure_file(${CMAKE_SOURCE_DIR}/src/init-ceph.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/init-ceph @ONLY)

configure_file(ceph-post-file.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-post-file @ONLY)

configure_file(ceph-crash.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-crash @ONLY)

# the src/.git_version file may be written out by make-dist; otherwise
# we pull the git version from .git
option(ENABLE_GIT_VERSION "build Ceph with git version string" ON)
if(ENABLE_GIT_VERSION)
  get_git_head_revision(GIT_REFSPEC CEPH_GIT_VER)
  git_describe(CEPH_GIT_NICE_VER_WITH_V --always)
  # remove leading 'v'
  string(SUBSTRING ${CEPH_GIT_NICE_VER_WITH_V} 1 -1 CEPH_GIT_NICE_VER)
  #if building from a source tarball via make-dist
  if(${CEPH_GIT_VER} STREQUAL "GITDIR-NOTFOUND")
    message(STATUS "Ceph/.git directory not found, parsing ${CMAKE_CURRENT_SOURCE_DIR}/.git_version for CEPH_GIT_VER and CEPH_GIT_NICE_VER")
    file(STRINGS ${CMAKE_CURRENT_SOURCE_DIR}/.git_version CEPH_GIT_SHA_AND_TAG)
    list(GET CEPH_GIT_SHA_AND_TAG 0 CEPH_GIT_VER)
    list(GET CEPH_GIT_SHA_AND_TAG 1 CEPH_GIT_NICE_VER)
  endif(${CEPH_GIT_VER} STREQUAL "GITDIR-NOTFOUND")
else(ENABLE_GIT_VERSION)
  set(CEPH_GIT_VER "no_version")
  set(CEPH_GIT_NICE_VER "Development")
endif(ENABLE_GIT_VERSION)

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

configure_file(${CMAKE_SOURCE_DIR}/src/ceph.in
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph @ONLY)

# Common infrastructure
configure_file(
  ${CMAKE_SOURCE_DIR}/src/ceph_ver.h.in.cmake
  ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h
  @ONLY)

add_definitions(
  -DHAVE_CONFIG_H
  -D__CEPH__
  -D_REENTRANT
  -D_THREAD_SAFE
  -D__STDC_FORMAT_MACROS
  -D_FILE_OFFSET_BITS=64
  -DBOOST_ASIO_DISABLE_THREAD_KEYWORD_EXTENSION
  -DBOOST_ASIO_NO_TS_EXECUTORS)

if(LINUX)
  add_definitions("-D_GNU_SOURCE")
endif()

add_compile_options(
  -Wall
  -fno-strict-aliasing
  -fsigned-char)

if(NOT MSVC)
  add_compile_options(
    -Wtype-limits
    -Wignored-qualifiers
    -Wpointer-arith
    -Werror=format-security
    -Winit-self
    -Wno-unknown-pragmas)
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wnon-virtual-dtor>)
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wno-ignored-qualifiers>)
endif()

add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-ftemplate-depth-1024>)

CHECK_CXX_COMPILER_FLAG("-Wpessimizing-move" COMPILER_SUPPORTS_PESSIMIZING_MOVE)
if(COMPILER_SUPPORTS_PESSIMIZING_MOVE)
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wpessimizing-move>)
endif()
CHECK_CXX_COMPILER_FLAG("-Wredundant-move" COMPILER_SUPPORTS_REDUNDANT_MOVE)
if(COMPILER_SUPPORTS_REDUNDANT_MOVE)
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wredundant-move>)
endif()
if(CMAKE_CXX_COMPILER_ID STREQUAL GNU)
  if(WIN32)
    # require >= gcc-10 for compiling the windows port
    set(minimum_gcc_version 10)
  else()
    # require >= gcc-11 for compiling the whole tree
    set(minimum_gcc_version 11)
  endif()
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS minimum_gcc_version)
    message(FATAL_ERROR "C++20 support requires a minimum GCC version of ${minimum_gcc_version}.")
  endif()
  if(MINGW)
    # The MINGW headers are missing some "const" qualifiers.
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-fpermissive>)
  else()
    string(APPEND CMAKE_EXE_LINKER_FLAGS " -rdynamic")
  endif()
  string(PREPEND CMAKE_CXX_FLAGS_DEBUG "-Og ")
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wstrict-null-sentinel>)
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Woverloaded-virtual>)
  # cmake does not add '-pie' for executables even if
  # CMAKE_POSITION_INDEPENDENT_CODE is TRUE.
  if(EXE_LINKER_USE_PIE)
    if (NOT WITH_OSD_INSTRUMENT_FUNCTIONS AND NOT HAVE_SEASTAR)
      string(APPEND CMAKE_EXE_LINKER_FLAGS " -pie")
    endif()
  endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL Clang)
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 12) # require >= clang-12
    message(FATAL_ERROR "C++20 support requires a minimum Clang version of 12.")
  endif()
  string(APPEND CMAKE_EXE_LINKER_FLAGS " ${CMAKE_EXE_EXPORTS_C_FLAG}")
  string(APPEND CMAKE_LINKER_FLAGS " -rdynamic -export-dynamic ${CMAKE_EXE_EXPORTS_C_FLAG}")
  string(PREPEND CMAKE_CXX_FLAGS_DEBUG "-g ")
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wno-inconsistent-missing-override>)
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wno-mismatched-tags>)
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wno-unused-private-field>)
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wno-address-of-packed-member>)
  add_compile_options(
    -Wno-unused-function
    -Wno-unused-local-typedef
    -Wno-varargs
    -Wno-gnu-designator
    -Wno-missing-braces
    -Wno-parentheses
    -Wno-deprecated-register)
  if(FREEBSD)
    # Need to use the GNU binutils linker to get versioning right.
    string(APPEND CMAKE_EXE_LINKER_FLAGS " -fuse-ld=/usr/local/bin/ld -Wno-unused-command-line-argument")
    string(APPEND CMAKE_SHARED_LINKER_FLAGS " -fuse-ld=/usr/local/bin/ld -Wno-unused-command-line-argument")
  endif()
  if(APPLE)
    string(APPEND CMAKE_SHARED_LINKER_FLAGS " -undefined dynamic_lookup")
  endif()
endif(CMAKE_CXX_COMPILER_ID STREQUAL GNU)

if(WITH_CEPH_DEBUG_MUTEX)
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-DCEPH_DEBUG_MUTEX>)
endif()

include(CheckCCompilerFlag)
if(CMAKE_CXX_COMPILER_ID STREQUAL GNU)
  CHECK_C_COMPILER_FLAG("-U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=2" HAS_FORTIFY_SOURCE)
  if(NOT CMAKE_BUILD_TYPE STREQUAL Debug)
    if(HAS_FORTIFY_SOURCE)
      add_definitions(
        -U_FORTIFY_SOURCE
        -D_FORTIFY_SOURCE=2)
    endif()
  endif()
    CHECK_C_COMPILER_FLAG(-fstack-protector-strong HAS_STACK_PROTECT)
    if (HAS_STACK_PROTECT)
      add_compile_options(-fstack-protector-strong)
      if(WIN32)
        add_link_options(-fstack-protector-strong)
      endif(WIN32)
    endif()
endif(CMAKE_CXX_COMPILER_ID STREQUAL GNU)

CHECK_C_COMPILER_FLAG("-D_GLIBCXX_ASSERTIONS" HAS_GLIBCXX_ASSERTIONS)
if(HAS_GLIBCXX_ASSERTIONS AND CMAKE_BUILD_TYPE STREQUAL Debug)
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-D_GLIBCXX_ASSERTIONS>)
endif()

include(SIMDExt)
if(HAVE_INTEL)
  if(APPLE)
    set(object_format "macho64")
  else()
    set(object_format "elf64")
  endif()
  set(CMAKE_ASM_FLAGS "-f ${object_format}")
  set(CMAKE_ASM_COMPILER ${PROJECT_SOURCE_DIR}/src/nasm-wrapper)
  if(NOT WIN32)
    # The native tools might be located even when cross compiling, which
    # might not work in this case (especially when targeting Windows).
    include(CheckNasm)
    check_nasm_support(${object_format}
      HAVE_NASM_X64
      HAVE_NASM_X64_AVX2
      HAVE_NASM_X64_AVX512)
  endif()
endif()

# require c++20
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 99)
# we use `asm()` to inline assembly, so enable the GNU extension
set(CMAKE_C_EXTENSIONS ON)
set(C_STANDARD_REQUIRED ON)

include(CheckCXXSourceCompiles)
CHECK_CXX_SOURCE_COMPILES("
#include <map>
using Map = std::map<int, int>;
int main() {
  Map m;
  m.merge(Map{});
}
" HAVE_STDLIB_MAP_SPLICING)

## Handle diagnostics color if compiler supports them.
CHECK_C_COMPILER_FLAG("-fdiagnostics-color=always"
  COMPILER_SUPPORTS_DIAGNOSTICS_COLOR)

set(DIAGNOSTICS_COLOR "auto"
  CACHE STRING "Used if the C/C++ compiler supports the -fdiagnostics-color option. May have one of three values -- 'auto' (default), 'always', or 'never'. If set to 'always' and the compiler supports the option, 'make [...] | less -R' will make visible diagnostics colorization of compiler output.")

if(COMPILER_SUPPORTS_DIAGNOSTICS_COLOR)
  add_compile_options(
    "$<$<COMPILE_LANGUAGE:C>:-fdiagnostics-color=${DIAGNOSTICS_COLOR}>"
    "$<$<COMPILE_LANGUAGE:CXX>:-fdiagnostics-color=${DIAGNOSTICS_COLOR}>")
endif()

if(MINGW)
  # Fedora and RHEL set CMAKE_DL_LIBS on MINGW, which we must un-set here.
  # Details at https://bugzilla.redhat.com/show_bug.cgi?id=2127529
  set(CMAKE_DL_LIBS "")
endif()

set(EXTRALIBS ${CMAKE_DL_LIBS})
if(HAVE_POSIX_TIMERS)
  list(APPEND EXTRALIBS ${RT_LIBRARY})
endif()
if(LINUX OR APPLE)
  set(LIB_RESOLV resolv)
  list(APPEND EXTRALIBS ${LIB_RESOLV})
endif()

option(ENABLE_COVERAGE "Coverage is enabled" OFF)
if(${ENABLE_COVERAGE})
  find_program(HAVE_GCOV gcov)
  if(NOT HAVE_GCOV)
    message(FATAL_ERROR "Coverage Enabled but gcov Not Found")
  endif()
  add_compile_options(
    --coverage
    -O0)
  add_link_options(
    --coverage
  )
  list(APPEND EXTRALIBS gcov)
endif(${ENABLE_COVERAGE})

if(WITH_TESTS)
  set(GCOV_PREFIX_STRIP 4)
  configure_file(${CMAKE_SOURCE_DIR}/src/ceph-coverage.in
    ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-coverage @ONLY)
  configure_file(${CMAKE_SOURCE_DIR}/src/ceph-debugpack.in
    ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-debugpack @ONLY)
endif()

option(WITH_OCF "build OCF-compliant cluster resource agent" OFF)
if(WITH_OCF)
  add_subdirectory(ocf)
endif()

option(WITH_CEPHFS_JAVA "build libcephfs Java bindings" OFF)
if(WITH_CEPHFS_JAVA)
  add_subdirectory(java)
endif()

if (WITH_BLKIN)
  add_subdirectory(blkin/blkin-lib)
endif(WITH_BLKIN)

if(WITH_JAEGER)
  find_package(thrift 0.13.0 REQUIRED)
  include(BuildOpentelemetry)
  build_opentelemetry()
  add_library(jaeger_base INTERFACE)
  target_link_libraries(jaeger_base INTERFACE opentelemetry::libopentelemetry
    thrift::libthrift)
endif()

set(mds_files)
list(APPEND mds_files
  mds/MDSMap.cc
  mds/MDSAuthCaps.cc
  mds/FSMap.cc
  mds/FSMapUser.cc
  mds/inode_backtrace.cc
  mds/mdstypes.cc
  mds/flock.cc
  mds/cephfs_features.cc)

add_subdirectory(json_spirit)

include_directories(SYSTEM "${CMAKE_SOURCE_DIR}/src/xxHash")

# use the rapidjson headers from s3select's submodule
if(NOT TARGET RapidJSON::RapidJSON)
  add_library(RapidJSON::RapidJSON INTERFACE IMPORTED)
  set_target_properties(RapidJSON::RapidJSON PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_SOURCE_DIR}/s3select/rapidjson/include")
endif()

option(WITH_FMT_HEADER_ONLY "use header-only version of fmt library" OFF)
option(WITH_SYSTEM_FMT "build against system fmt" OFF)
if(WITH_SYSTEM_FMT)
  find_package(fmt 8.1.1...<10.0.0 REQUIRED)
endif()
if (WITH_FMT_HEADER_ONLY)
  message(STATUS "Using fmt header-only.")
  set(FMT_LIB fmt::fmt-header-only)
else()
  message(STATUS "Linking to fmt library.")
  set(FMT_LIB fmt::fmt)
endif()
if(fmt_FOUND)
  message(STATUS "Building with system fmt.")
else()
  message(STATUS "Building fmt as submodule")
  set(old_BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS})
  set(BUILD_SHARED_LIBS FALSE)
  add_subdirectory(fmt)
  set(BUILD_SHARED_LIBS ${old_BUILD_SHARED_LIBS})
  unset(old_BUILD_SHARED_LIBS)
  include_directories(SYSTEM "${CMAKE_SOURCE_DIR}/src/fmt/include")
endif()

# in osd/PeeringState.h, the number of elements in PeeringState::Active::reactions
# is now 21 which exceeds the default value of BOOST_MPL_LIMIT_VECTOR_SIZE, which
# is 20. so we need to override it. see
# https://www.boost.org/doc/libs/1_74_0/libs/mpl/doc/refmanual/limit-list-size.html
# link with this library, if your code includes osd/PeeringState.h (indirectly)
add_library(Boost::MPL INTERFACE IMPORTED)
set_target_properties(Boost::MPL PROPERTIES
  INTERFACE_COMPILE_DEFINITIONS
    "BOOST_MPL_CFG_NO_PREPROCESSED_HEADERS;BOOST_MPL_LIMIT_LIST_SIZE=30")

if(WITH_SEASTAR)
  find_package(c-ares 1.13.0 QUIET)
  if(NOT c-ares_FOUND)
    message(STATUS "Could not find c-ares, will build it")
    include(Buildc-ares)
    build_c_ares()
  endif()
  macro(find_package name)
    if(NOT TARGET ${name})
      _find_package(${ARGV})
    endif()
  endmacro ()
  set(Seastar_API_LEVEL "6" CACHE STRING "" FORCE)
  set(Seastar_HWLOC OFF CACHE BOOL "" FORCE)
  set(Seastar_IO_URING OFF CACHE BOOL "" FORCE)
  set(Seastar_STD_OPTIONAL_VARIANT_STRINGVIEW ON CACHE BOOL "" FORCE)
  if(Seastar_DPDK)
    find_package(dpdk QUIET)
    if(NOT DPDK_FOUND)
      include(BuildDPDK)
      build_dpdk(${CMAKE_BINARY_DIR}/src/dpdk)
    endif()
  endif()
  list(APPEND Seastar_CXX_FLAGS
    "-DSEASTAR_NO_EXCEPTION_HACK"
    "-Wno-error"
    "-Wno-sign-compare"
    "-Wno-attributes"
    "-Wno-pessimizing-move"
    "-Wno-address-of-packed-member"
    "-Wno-non-virtual-dtor")
  set(Seastar_CXX_FLAGS "${Seastar_CXX_FLAGS}" CACHE STRING "" FORCE)
  add_subdirectory(seastar)
  # create the directory so cmake won't complain when looking at the imported
  # target: Seastar exports this directory created at build-time
  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/seastar/gen/include")
  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/seastar/gen/src")
  add_subdirectory(crimson)
endif()

include(CheckTypeSize)
set(CMAKE_EXTRA_INCLUDE_FILES "sys/time.h")
CHECK_TYPE_SIZE(suseconds_t SUSECONDS_T)
unset(CMAKE_EXTRA_INCLUDE_FILES)

set(libcommon_files
  ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h
  ceph_ver.c
  global/global_context.cc
  xxHash/xxhash.c
  common/error_code.cc
  common/tracer.cc
  log/Log.cc
  mon/MonCap.cc
  mon/MonClient.cc
  mon/MonMap.cc
  mon/MonSub.cc
  mon/error_code.cc
  mgr/MgrClient.cc
  mon/PGMap.cc
  mgr/ServiceMap.cc
  osd/ECMsgTypes.cc
  osd/HitSet.cc
  osd/OSDMap.cc
  osd/OSDMapMapping.cc
  osd/osd_types.cc
  osd/error_code.cc
  osd/PGPeeringEvent.cc
  osd/OpRequest.cc
  osd/ClassHandler.cc
  osd/osd_op_util.cc
  osdc/Striper.cc
  osdc/Objecter.cc
  osdc/error_code.cc
  librbd/Features.cc
  librbd/io/IoOperations.cc
  ${mds_files})

set_source_files_properties(ceph_ver.c
  APPEND PROPERTY OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h)
add_library(common-objs OBJECT ${libcommon_files})
target_link_libraries(common-objs legacy-option-headers)
target_compile_definitions(common-objs PRIVATE
  $<TARGET_PROPERTY:${FMT_LIB},INTERFACE_COMPILE_DEFINITIONS>)

if(WITH_JAEGER)
  add_dependencies(common-objs jaeger_base)
  target_link_libraries(common-objs jaeger_base)
endif()

CHECK_C_COMPILER_FLAG("-fvar-tracking-assignments" HAS_VTA)
add_subdirectory(auth)
add_subdirectory(common)
add_subdirectory(crush)
add_subdirectory(msg)
add_subdirectory(arch)
add_subdirectory(extblkdev)

set(ceph_common_objs
  $<TARGET_OBJECTS:common-auth-objs>
  $<TARGET_OBJECTS:common-common-objs>
  $<TARGET_OBJECTS:common-options-objs>
  $<TARGET_OBJECTS:common-msg-objs>
  $<TARGET_OBJECTS:common_buffer_obj>
  $<TARGET_OBJECTS:common_texttable_obj>
  $<TARGET_OBJECTS:compressor_objs>
  $<TARGET_OBJECTS:common-objs>
  $<TARGET_OBJECTS:common_mountcephfs_objs>
  $<TARGET_OBJECTS:crush_objs>)
set(ceph_common_deps
  json_spirit erasure_code extblkdev arch crc32
  ${LIB_RESOLV}
  Boost::thread
  Boost::system
  Boost::random
  Boost::program_options
  Boost::date_time
  Boost::iostreams
  StdFilesystem::filesystem
  ${FMT_LIB}
  ${BLKID_LIBRARIES}
  ${Backtrace_LIBRARIES}
  ${BLKIN_LIBRARIES}
  ${CRYPTO_LIBS}
  ${GSSAPI_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT}
  ${CMAKE_DL_LIBS})
if(HAVE_UDEV)
  list(APPEND ceph_common_deps ${UDEV_LIBRARIES})
endif()

if(HAVE_VERBS)
  list(APPEND ceph_common_deps IBVerbs::verbs)
endif()

if(HAVE_RDMACM)
  list(APPEND ceph_common_deps RDMA::RDMAcm)
endif()

if(NOT WITH_SYSTEM_BOOST)
  list(APPEND ceph_common_deps ${ZLIB_LIBRARIES})
endif()

if(WITH_DPDK)
  list(APPEND ceph_common_deps common_async_dpdk)
endif()

if(WITH_JAEGER)
  list(APPEND ceph_common_deps jaeger_base)
endif()

if(WIN32)
  list(APPEND ceph_common_deps ws2_32 mswsock iphlpapi bcrypt)
  list(APPEND ceph_common_deps dlfcn_win32)
endif()

if(WITH_BLUESTORE_PMEM OR WITH_RBD_RWL)
  if(WITH_SYSTEM_PMDK)
    set(pmdk_COMPONENTS)
    if(WITH_BLUESTORE_PMEM)
      list(APPEND pmdk_COMPONENTS pmem)
    endif()
    if(WITH_RBD_RWL)
      list(APPEND pmdk_COMPONENTS pmemobj)
    endif()
    find_package(pmdk 1.8 REQUIRED COMPONENTS ${pmdk_COMPONENTS})
  else()
    include(Buildpmdk)
    if(WITH_BLUESTORE_PMEM)
      set(enable_ndctl ON)
      find_package(ndctl 63 REQUIRED)
      find_package(daxctl 63 REQUIRED)
    else()
      set(enable_ndctl OFF)
    endif()
    build_pmdk(${enable_ndctl})
  endif()
endif()

add_library(common STATIC ${ceph_common_objs})
target_link_libraries(common
  ${ceph_common_deps}
  legacy-option-headers)
if(WITH_JAEGER)
add_dependencies(common jaeger_base)
endif()

if (WIN32)
  # Statically building ceph-common on Windows fails. We're temporarily
  # reverting this: 22fefb2338cfc4fcb03ece3cbf77aa964a7f17f2
  add_library(ceph-common SHARED ${ceph_common_objs})
else()
  add_library(ceph-common ${CEPH_SHARED} ${ceph_common_objs})
endif()

target_link_libraries(ceph-common ${ceph_common_deps})
if(ENABLE_COVERAGE)
  target_link_libraries(ceph-common gcov)
endif(ENABLE_COVERAGE)

target_link_libraries(ceph-common legacy-option-headers)

if(WITH_JAEGER)
add_dependencies(ceph-common jaeger_base)
endif()

# appease dpkg-shlibdeps
set_target_properties(ceph-common PROPERTIES
  SOVERSION 2
  SKIP_RPATH TRUE)
if(NOT APPLE AND NOT FREEBSD AND NOT WIN32)
  # Apple uses Mach-O, not ELF. so this option does not apply to APPLE.
  #
  # prefer the local symbol definitions when binding references to global
  # symbols. otherwise we could reference the symbols defined by the application
  # with the same name, instead of using the one defined in libceph-common.
  # in other words, we require libceph-common to use local symbols, even if redefined
  # in application".
  set_property(
    TARGET ceph-common
    APPEND APPEND_STRING
    PROPERTY LINK_FLAGS "-Wl,-Bsymbolic -Wl,-Bsymbolic-functions")
endif()

if(MINGW)
  install(
    TARGETS ceph-common
    RUNTIME
    DESTINATION ${CEPH_INSTALL_PKGLIBDIR})
else()
  install(
    TARGETS ceph-common
    LIBRARY
    DESTINATION ${CEPH_INSTALL_PKGLIBDIR}
    NAMELINK_SKIP)
endif()

if(${WITH_LTTNG})
  add_subdirectory(tracing)
  add_dependencies(common-objs oprequest-tp)
endif(${WITH_LTTNG})

add_subdirectory(global)

# RGW also support Lua scripting
if(WITH_CEPHFS OR WITH_RADOSGW)
  find_package(Lua 5.3 REQUIRED)
endif()

# rados object classes
add_subdirectory(cls)

# RADOS client/library
add_subdirectory(osdc)

# heal_profiler
add_subdirectory(perfglue)

add_library(rados_snap_set_diff_obj OBJECT librados/snap_set_diff.cc)
target_link_libraries(rados_snap_set_diff_obj
  PRIVATE legacy-option-headers)

option(WITH_LIBRADOSSTRIPER "build with libradosstriper support" ON)

add_subdirectory(include)
add_subdirectory(librados)
add_subdirectory(neorados)

if(WITH_LIBRADOSSTRIPER)
  add_subdirectory(libradosstriper)
endif()

# make rocksdb statically

if(NOT WITH_SYSTEM_ROCKSDB)
  include(BuildRocksDB)
  build_rocksdb()
endif(NOT WITH_SYSTEM_ROCKSDB)

if(WITH_MGR)
  add_subdirectory(mgr)
  add_subdirectory(exporter)
endif()

set(librados_config_srcs
  librados-config.cc)
add_executable(librados-config ${librados_config_srcs})
target_link_libraries(librados-config librados Boost::program_options)

install(TARGETS librados-config DESTINATION bin)

# virtualenv base directory for ceph-disk and ceph-detect-init
set(CEPH_BUILD_VIRTUALENV $ENV{TMPDIR})
if(NOT CEPH_BUILD_VIRTUALENV)
  set(CEPH_BUILD_VIRTUALENV ${CMAKE_BINARY_DIR})
endif()

if(NOT WIN32)
add_subdirectory(pybind)
add_subdirectory(ceph-volume)
add_subdirectory(ceph-node-proxy)
add_subdirectory(python-common)
add_subdirectory(cephadm)
endif(NOT WIN32)

# Monitor
add_subdirectory(mon)
set(ceph_mon_srcs
  ceph_mon.cc)
add_executable(ceph-mon ${ceph_mon_srcs}
  $<TARGET_OBJECTS:common_texttable_obj>)
add_dependencies(ceph-mon erasure_code_plugins)
target_link_libraries(ceph-mon mon os global-static ceph-common
  ${ALLOC_LIBS}
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  ${GSSAPI_LIBRARIES})
install(TARGETS ceph-mon DESTINATION bin)

# OSD/ObjectStore

include(TestBigEndian)
test_big_endian(CEPH_BIG_ENDIAN)
if(NOT CEPH_BIG_ENDIAN)
  set(CEPH_LITTLE_ENDIAN 1)
endif()

add_subdirectory(kv)
add_subdirectory(os)

if(NOT WIN32)
add_subdirectory(blk)
add_subdirectory(osd)

set(ceph_osd_srcs
  # Link the Object Class API implementation directly as intermediary
  # static library (like libosd.a) nullifies the effect of `-rdynamic`.
  osd/objclass.cc
  objclass/class_api.cc
  ceph_osd.cc)

add_executable(ceph-osd ${ceph_osd_srcs})
add_dependencies(ceph-osd erasure_code_plugins extblkdev_plugins)
target_link_libraries(ceph-osd osd os global-static common
  ${ALLOC_LIBS}
  ${BLKID_LIBRARIES})
if(WITH_FUSE)
  target_link_libraries(ceph-osd FUSE::FUSE)
endif()
set_target_properties(ceph-osd PROPERTIES
  POSITION_INDEPENDENT_CODE ${EXE_LINKER_USE_PIE}
  INSTALL_RPATH "")
install(TARGETS ceph-osd DESTINATION bin)

endif(NOT WIN32)

if (WITH_CEPHFS)
  add_subdirectory(mds)
  set(ceph_mds_srcs
    ceph_mds.cc)
  add_executable(ceph-mds ${ceph_mds_srcs})
  target_link_libraries(ceph-mds mds ${CMAKE_DL_LIBS} global-static ceph-common
    Boost::thread
    ${ALLOC_LIBS})
  install(TARGETS ceph-mds DESTINATION bin)
endif()

add_subdirectory(erasure-code)

# Support/Tools
if(WITH_TESTS)
  option(WITH_SYSTEM_GTEST "require and build with system gtest and gmock" OFF)
  if(WITH_SYSTEM_GTEST)
    find_package(GTest REQUIRED)
    find_package(GMock REQUIRED)
  else()
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    add_subdirectory(googletest)
    add_library(GMock::GMock ALIAS gmock)
    add_library(GMock::Main ALIAS gmock_main)
    target_include_directories(gmock INTERFACE
      $<TARGET_PROPERTY:gtest,INTERFACE_INCLUDE_DIRECTORIES>)
    target_include_directories(gmock_main INTERFACE
      $<TARGET_PROPERTY:gtest,INTERFACE_INCLUDE_DIRECTORIES>)
    add_library(GTest::GTest ALIAS gtest)
    add_library(GTest::Main ALIAS gtest_main)
  endif()
endif(WITH_TESTS)

# dmClock (after gmock)
option(WITH_DMCLOCK_TESTS
  "enable the build of dmclock-tests and dmclock-data-struct tests binaries"
  OFF)
if(WITH_TESTS AND WITH_DMCLOCK_TESTS)
  # note: add_test is not being called, so dmclock tests aren't part
  # of ceph tests
  set(dmclock_TEST ON CACHE BOOL "" FORCE)
endif()
add_subdirectory(dmclock)

add_subdirectory(compressor)

add_subdirectory(tools)

if(WITH_TESTS)
  add_subdirectory(test)
endif()

add_subdirectory(crypto)

if(WITH_TESTS)
  install(PROGRAMS
    ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-debugpack
    ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-coverage
    DESTINATION bin)
endif()

install(PROGRAMS
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-post-file
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph-crash
  ceph-run
  ceph-clsinfo
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
  ceph-create-keys
  DESTINATION sbin)

add_subdirectory(bash_completion)
add_subdirectory(client)

if(WITH_LIBCEPHFS)
  set(libcephfs_srcs libcephfs.cc)
  add_library(cephfs ${CEPH_SHARED} ${libcephfs_srcs})
  target_link_libraries(cephfs PRIVATE client ceph-common
    ${CRYPTO_LIBS} ${EXTRALIBS})
  if(ENABLE_SHARED)
    set_target_properties(cephfs PROPERTIES
      OUTPUT_NAME cephfs
      VERSION 2.0.0
      SOVERSION 2)
    if(NOT APPLE AND NOT
        (WIN32 AND CMAKE_CXX_COMPILER_ID STREQUAL Clang))
      foreach(name ceph-common client osdc)
        set_property(TARGET cephfs APPEND_STRING PROPERTY
          LINK_FLAGS " -Wl,--exclude-libs,lib${name}.a")
      endforeach()
    endif()
  endif(ENABLE_SHARED)
  install(TARGETS cephfs DESTINATION ${CMAKE_INSTALL_LIBDIR})
  install(DIRECTORY
    "include/cephfs"
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
  set(ceph_syn_srcs
    ceph_syn.cc
    client/SyntheticClient.cc)
  add_executable(ceph-syn ${ceph_syn_srcs})
  target_link_libraries(ceph-syn client global-static ceph-common)
  install(TARGETS ceph-syn DESTINATION bin)
  if(LINUX)
    add_subdirectory(mount)
  endif()
endif(WITH_LIBCEPHFS)

if(WITH_LIBCEPHSQLITE)
  set(cephsqlite_srcs libcephsqlite.cc SimpleRADOSStriper.cc)
  add_library(cephsqlite ${CEPH_SHARED} ${cephsqlite_srcs})
  target_link_libraries(cephsqlite PRIVATE cls_lock_client librados ceph-common SQLite3::SQLite3 ${EXTRALIBS})
  set_target_properties(cephsqlite PROPERTIES
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON)
  if(NOT APPLE AND NOT WIN32)
    set_property(TARGET cephsqlite APPEND_STRING PROPERTY
      LINK_FLAGS " -Wl,--exclude-libs,ALL")
  endif()
  install(TARGETS cephsqlite DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif(WITH_LIBCEPHSQLITE)

if(WITH_FUSE)
  set(ceph_fuse_srcs
    ceph_fuse.cc
    client/fuse_ll.cc)
  add_executable(ceph-fuse ${ceph_fuse_srcs})
  target_link_libraries(ceph-fuse FUSE::FUSE
    ${GSSAPI_LIBRARIES} client ceph-common global-static ${EXTRALIBS})
  set_target_properties(ceph-fuse PROPERTIES
    POSITION_INDEPENDENT_CODE ${EXE_LINKER_USE_PIE})
  install(TARGETS ceph-fuse DESTINATION bin)
  install(PROGRAMS mount.fuse.ceph DESTINATION ${CMAKE_INSTALL_SBINDIR})
endif(WITH_FUSE)

if(WITH_DOKAN)
  add_subdirectory(dokan)
endif(WITH_DOKAN)

add_subdirectory(journal)

if(WITH_RBD)
  if(WITH_KRBD)
    add_library(krbd STATIC krbd.cc
      $<TARGET_OBJECTS:parse_secret_objs>)
    target_link_libraries(krbd
      keyutils::keyutils
      legacy-option-headers)
  endif()
  add_subdirectory(librbd)
  if(WITH_FUSE)
    add_subdirectory(rbd_fuse)
  endif()

  install(PROGRAMS
    ceph-rbdnamer
    rbd-replay-many
    rbdmap
    DESTINATION ${CMAKE_INSTALL_BINDIR})
  add_subdirectory(rbd_replay)
endif(WITH_RBD)

# RadosGW
if(WITH_KVS)
  add_subdirectory(key_value_store)
endif(WITH_KVS)

if(WITH_RADOSGW)
  if(WITH_RADOSGW_SELECT_PARQUET OR WITH_RADOSGW_ARROW_FLIGHT)
    if(WITH_SYSTEM_ARROW)
      find_package(Arrow 4 REQUIRED QUIET)
      find_package(Parquet 4 REQUIRED QUIET)
    else()
      # find arrow's dependencies
      if (WITH_SYSTEM_UTF8PROC)
        find_package(utf8proc 2.2.0 REQUIRED)
      else()
        include(BuildUtf8proc)
        build_utf8proc()
      endif()
      find_package(thrift 0.13 REQUIRED)

      include(BuildArrow)
      build_arrow()
    endif(WITH_SYSTEM_ARROW)
  endif(WITH_RADOSGW_SELECT_PARQUET OR WITH_RADOSGW_ARROW_FLIGHT)

  add_subdirectory(libkmip)
  add_subdirectory(rgw)
endif(WITH_RADOSGW)

install(FILES
  sample.ceph.conf
  DESTINATION ${CMAKE_INSTALL_DOCDIR})

# Now create a usable config.h
configure_file(
  ${CMAKE_SOURCE_DIR}/src/include/config-h.in.cmake
  ${CMAKE_BINARY_DIR}/include/acconfig.h
)

# Everything you need to spin up a cluster with vstart.sh
add_custom_target(vstart-base DEPENDS
    ceph-osd
    ceph-mon
    ceph-authtool
    ceph-conf
    monmaptool
    crushtool
    rados)
foreach(dep
    cython_rados
    ceph-mgr
    ceph-exporter)
  if(TARGET ${dep})
    add_dependencies(vstart-base ${dep})
  endif()
endforeach()

add_custom_target(vstart DEPENDS vstart-base)
foreach(dep
    cython_rbd
    cephfs
    cython_cephfs
    ceph-mds
    mgr-dashboard-frontend-build
    radosgw
    radosgw-admin
    tracepoint_libraries
    ceph-volume-venv-setup
    cephadm)
  if(TARGET ${dep})
    add_dependencies(vstart ${dep})
  endif()
endforeach()

# Everything you need to run CephFS tests
add_custom_target(cephfs_testing DEPENDS
    vstart
    rados
    cython_modules
    cephfs
    cls_cephfs
    ceph-fuse
    ceph-dencoder
    ceph-dencoder-modules
    cephfs-journal-tool
    cephfs-meta-injection
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

# add doxygen target
find_package(Doxygen)
if(DOXYGEN_FOUND)
  set(DOXYGEN_FILE_PATTERNS *.cc *.c *.cpp *.C *.cxx *.c++ *.CC *.H *.h *.hh *.hpp)
  set(DOXYGEN_SOURCE_BROWSER YES)
  set(DOXYGEN_WARN_IF_UNDOCUMENTED NO)
  # enabling clang slows down doxygen significantly
  set(DOXYGEN_CLANG_ASSISTED_PARSING NO)
  set(DOXYGEN_CLANG_DATABASE_PATH "${PROJECT_BINARY_DIR}")
  set(DOXYGEN_BUILTIN_STL_SUPPORT YES)
  set(DOXYGEN_RECURSIVE YES)
  set(DOXYGEN_QUIET YES)
  set(DOXYGEN_GENERATE_LATEX NO)
  set(DOXYGEN_GENERATE_XML NO)
  set(DOXYGEN_GENERATE_HTML YES)
  set(DOXYGEN_PROJECT_NAME Ceph)
  set(DOXYGEN_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/build-doc/doxygen")
  set(DOXYGEN_STRIP_FROM_PATH "src/")
  set(DOXYGEN_STRIP_FROM_INC_PATH "src/include")
  set(DOXYGEN_JAVADOC_AUTOBRIEF YES)
  set(DOXYGEN_HAVE_DOT NO)
  set(DOXYGEN_VERBATIM_HEADERS YES)
  doxygen_add_docs(doxygen
    auth
    client
    cls
    common
    compressor
    crimson
    crush
    crypto
    erasure-code
    global
    include
    journal
    json_spirit
    key_value_store
    kv
    librados
    libradosstriper
    librbd
    log
    mds
    messages
    mgr
    mon
    mount
    msg
    objclass
    objsync
    os
    osd
    osdc
    perfglue
    rbd_fuse
    rbd_replay
    rgw
    COMMENT "Generate C++ documentation")
endif()
