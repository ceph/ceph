cmake_minimum_required(VERSION 3.5.1)

project(ceph CXX C ASM)
set(VERSION 15.0.0)

if(POLICY CMP0028)
  cmake_policy(SET CMP0028 NEW)
endif()
if(POLICY CMP0046)
  cmake_policy(SET CMP0046 NEW)
endif()
if(POLICY CMP0054)
  cmake_policy(SET CMP0054 NEW)
endif()
if(POLICY CMP0056)
  cmake_policy(SET CMP0056 NEW)
endif()
if(POLICY CMP0065)
  cmake_policy(SET CMP0065 NEW)
endif()
if(POLICY CMP0051)
  cmake_policy(SET CMP0051 NEW)
endif()
if(POLICY CMP0075)
  cmake_policy(SET CMP0075 NEW)
endif()
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules/")

if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  set(LINUX ON)
  FIND_PACKAGE(Threads)
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  set(FREEBSD ON)
  FIND_PACKAGE(Threads)
endif(CMAKE_SYSTEM_NAME MATCHES "Linux")

if(CMAKE_CXX_COMPILER_ID STREQUAL GNU)
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7)
    message(FATAL_ERROR "GCC 7+ required due to C++17 requirements")
  endif()
endif()

option(WITH_CCACHE "Build with ccache.")
if(WITH_CCACHE)
  find_program(CCACHE_FOUND ccache)
  if(CCACHE_FOUND)
    message(STATUS "Building with ccache: ${CCACHE_FOUND}, CCACHE_DIR=$ENV{CCACHE_DIR}")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    # ccache does not accelerate link (ld), but let it handle it. by passing it
    # along with cc to python's distutils, we are able to workaround
    # https://bugs.python.org/issue8027.
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
  else(CCACHE_FOUND)
    message(FATAL_ERROR "Can't find ccache. Is it installed?")
  endif(CCACHE_FOUND)
endif(WITH_CCACHE)

option(WITH_MANPAGE "Build man pages." ON)
if(WITH_MANPAGE)
  find_program(SPHINX_BUILD
    NAMES sphinx-build sphinx-build-3)
  if(NOT SPHINX_BUILD)
    message(FATAL_ERROR "Can't find sphinx-build.")
  endif(NOT SPHINX_BUILD)
endif(WITH_MANPAGE)

include_directories(
  ${PROJECT_BINARY_DIR}/src/include
  ${PROJECT_SOURCE_DIR}/src)

if(OFED_PREFIX)
  include_directories(SYSTEM ${OFED_PREFIX}/include)
  link_directories(${OFED_PREFIX}/lib)
endif()

if(FREEBSD)
  include_directories(SYSTEM /usr/local/include)
  link_directories(/usr/local/lib)
  list(APPEND CMAKE_REQUIRED_INCLUDES /usr/local/include)
endif(FREEBSD)

#Check Includes
include(CheckIncludeFiles)
include(CheckIncludeFileCXX)
include(CheckFunctionExists)

#put all the libs and binaries in one place
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)


CHECK_FUNCTION_EXISTS(fallocate CEPH_HAVE_FALLOCATE)
CHECK_FUNCTION_EXISTS(posix_fadvise HAVE_POSIX_FADVISE)
CHECK_FUNCTION_EXISTS(posix_fallocate HAVE_POSIX_FALLOCATE)
CHECK_FUNCTION_EXISTS(syncfs HAVE_SYS_SYNCFS)
CHECK_FUNCTION_EXISTS(sync_file_range HAVE_SYNC_FILE_RANGE)
CHECK_FUNCTION_EXISTS(pwritev HAVE_PWRITEV)
CHECK_FUNCTION_EXISTS(splice CEPH_HAVE_SPLICE)
CHECK_FUNCTION_EXISTS(getgrouplist HAVE_GETGROUPLIST)
if(NOT APPLE)
  CHECK_FUNCTION_EXISTS(fdatasync HAVE_FDATASYNC)
endif()
CHECK_FUNCTION_EXISTS(strerror_r HAVE_Strerror_R)
CHECK_FUNCTION_EXISTS(name_to_handle_at HAVE_NAME_TO_HANDLE_AT)
CHECK_FUNCTION_EXISTS(pipe2 HAVE_PIPE2)
CHECK_FUNCTION_EXISTS(accept4 HAVE_ACCEPT4)

include(CMakePushCheckState)
cmake_push_check_state(RESET)
set(CMAKE_REQUIRED_LIBRARIES pthread)
CHECK_FUNCTION_EXISTS(pthread_spin_init HAVE_PTHREAD_SPINLOCK)
CHECK_FUNCTION_EXISTS(pthread_set_name_np HAVE_PTHREAD_SET_NAME_NP)
CHECK_FUNCTION_EXISTS(pthread_get_name_np HAVE_PTHREAD_GET_NAME_NP)
CHECK_FUNCTION_EXISTS(pthread_setname_np HAVE_PTHREAD_SETNAME_NP)
CHECK_FUNCTION_EXISTS(pthread_getname_np HAVE_PTHREAD_GETNAME_NP)
CHECK_FUNCTION_EXISTS(pthread_rwlockattr_setkind_np HAVE_PTHREAD_RWLOCKATTR_SETKIND_NP)
cmake_pop_check_state()

CHECK_FUNCTION_EXISTS(eventfd HAVE_EVENTFD)
CHECK_FUNCTION_EXISTS(getprogname HAVE_GETPROGNAME)

CHECK_INCLUDE_FILES("linux/types.h" HAVE_LINUX_TYPES_H)
CHECK_INCLUDE_FILES("linux/version.h" HAVE_LINUX_VERSION_H)
CHECK_INCLUDE_FILES("arpa/nameser_compat.h" HAVE_ARPA_NAMESER_COMPAT_H)
CHECK_INCLUDE_FILES("sys/mount.h" HAVE_SYS_MOUNT_H)
CHECK_INCLUDE_FILES("sys/param.h" HAVE_SYS_PARAM_H)
CHECK_INCLUDE_FILES("sys/types.h" HAVE_SYS_TYPES_H)
CHECK_INCLUDE_FILES("sys/vfs.h" HAVE_SYS_VFS_H)
CHECK_INCLUDE_FILES("sys/prctl.h" HAVE_SYS_PRCTL_H)
CHECK_INCLUDE_FILES("execinfo.h" HAVE_EXECINFO_H)
if(LINUX)
  CHECK_INCLUDE_FILES("sched.h" HAVE_SCHED)
endif(LINUX)
CHECK_INCLUDE_FILES("valgrind/helgrind.h" HAVE_VALGRIND_HELGRIND_H)

include(CheckTypeSize)
set(CMAKE_EXTRA_INCLUDE_FILES "linux/types.h")
CHECK_TYPE_SIZE(__be16 __BE16) 
CHECK_TYPE_SIZE(__be32 __BE32) 
CHECK_TYPE_SIZE(__be64 __BE64) 
CHECK_TYPE_SIZE(__le16 __LE16) 
CHECK_TYPE_SIZE(__le32 __LE32) 
CHECK_TYPE_SIZE(__le64 __LE64) 
CHECK_TYPE_SIZE(__u8 __U8) 
CHECK_TYPE_SIZE(__u16 __U16) 
CHECK_TYPE_SIZE(__u32 __U32) 
CHECK_TYPE_SIZE(__u64 __U64) 
CHECK_TYPE_SIZE(__s8 __S8) 
CHECK_TYPE_SIZE(__s16 __S16) 
CHECK_TYPE_SIZE(__s32 __S32) 
CHECK_TYPE_SIZE(__s64 __S64) 
unset(CMAKE_EXTRA_INCLUDE_FILES)

include(CheckSymbolExists)
CHECK_SYMBOL_EXISTS(res_nquery "resolv.h" HAVE_RES_NQUERY)
CHECK_SYMBOL_EXISTS(F_SETPIPE_SZ "linux/fcntl.h" CEPH_HAVE_SETPIPE_SZ)
CHECK_SYMBOL_EXISTS(__func__ "" HAVE_FUNC)
CHECK_SYMBOL_EXISTS(__PRETTY_FUNCTION__ "" HAVE_PRETTY_FUNC)
CHECK_SYMBOL_EXISTS(getentropy "unistd.h" HAVE_GETENTROPY)

include(CheckCXXSourceCompiles)
CHECK_CXX_SOURCE_COMPILES("
  #include <string.h>
  int main() { char x = *strerror_r(0, &x, sizeof(x)); return 0; }
  " STRERROR_R_CHAR_P)

include(CheckStructHasMember)
CHECK_STRUCT_HAS_MEMBER("struct stat" st_mtim.tv_nsec sys/stat.h
  HAVE_STAT_ST_MTIM_TV_NSEC LANGUAGE C)
CHECK_STRUCT_HAS_MEMBER("struct stat" st_mtimespec.tv_nsec sys/stat.h
  HAVE_STAT_ST_MTIMESPEC_TV_NSEC LANGUAGE C)

set(CEPH_MAN_DIR "share/man" CACHE STRING "Install location for man pages (relative to prefix).")

option(ENABLE_SHARED "build shared libraries" ON)
if(ENABLE_SHARED)
  set(CEPH_SHARED SHARED)
else(ENABLE_SHARED)
  set(CEPH_SHARED STATIC)
endif(ENABLE_SHARED)
set(CMAKE_POSITION_INDEPENDENT_CODE ${ENABLE_SHARED})

option(WITH_STATIC_LIBSTDCXX "Link against libstdc++ statically" OFF)
if(WITH_STATIC_LIBSTDCXX)
  if(CMAKE_COMPILER_IS_GNUCXX)
    set(static_linker_flags "-static-libstdc++ -static-libgcc")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${static_linker_flags}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${static_linker_flags}")
    unset(static_linker_flags)
    set(GPERFTOOLS_USE_STATIC_LIBS TRUE)
  else()
    message(FATAL_ERROR "Please use GCC to enable WITH_STATIC_LIBSTDCXX")
  endif()
endif()
include(CheckCxxAtomic)
if(NOT HAVE_CXX11_ATOMIC)
  set(CMAKE_CXX_STANDARD_LIBRARIES
    "${CMAKE_CXX_STANDARD_LIBRARIES} ${LIBATOMIC_LINK_FLAGS}")
endif()

option(WITH_RDMA "Enable RDMA in async messenger" ON)
if(WITH_RDMA)
  find_package(verbs REQUIRED)
  set(HAVE_VERBS ${VERBS_FOUND})
  find_package(rdmacm REQUIRED)
  set(HAVE_RDMACM ${RDMACM_FOUND})
  set(HAVE_RDMA TRUE)
endif()

find_package(Backtrace)

if(LINUX)
  find_package(udev REQUIRED)
  set(HAVE_UDEV ${UDEV_FOUND})
  find_package(blkid REQUIRED)
  set(HAVE_BLKID ${BLKID_FOUND})
elseif(FREEBSD)
  set(HAVE_UDEV OFF)
  set(HAVE_LIBAIO OFF)
  set(HAVE_BLKID OFF)
else()
  set(HAVE_UDEV OFF)
  message(STATUS "Not using udev")
  set(HAVE_BLKID OFF)
  message(STATUS "Not using BLKID")
endif(LINUX)

option(WITH_OPENLDAP "OPENLDAP is here" ON)
if(WITH_OPENLDAP)
  find_package(OpenLdap REQUIRED)
  set(HAVE_OPENLDAP ${OPENLDAP_FOUND})
endif()

option(WITH_GSSAPI "GSSAPI/KRB5 is here" OFF)
if(WITH_GSSAPI)
  find_package(GSSApi REQUIRED)
  set(HAVE_GSSAPI ${GSSApi_FOUND})
endif()

option(WITH_FUSE "Fuse is here" ON)
if(WITH_FUSE)
  find_package(fuse)
  set(HAVE_LIBFUSE ${FUSE_FOUND})
endif()

option(WITH_XFS "XFS is here" ON)
if(WITH_XFS)
  find_package(xfs)
  set(HAVE_LIBXFS ${XFS_FOUND})
endif()

option(WITH_ZFS "enable LibZFS if found" OFF)
if(WITH_ZFS)
  find_package(zfs)
  set(HAVE_LIBZFS ${ZFS_FOUND})
endif()

option(WITH_BLUESTORE "Bluestore OSD backend" ON)
if(WITH_BLUESTORE)
  if(LINUX)
    find_package(aio)
    set(HAVE_LIBAIO ${AIO_FOUND})
  elseif(FREEBSD)
    # POSIX AIO is integrated into FreeBSD kernel, and exposed by libc.
    set(HAVE_POSIXAIO ON)
  endif()
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "i386|i686|amd64|x86_64|AMD64|aarch64")
  option(WITH_SPDK "Enable SPDK" ON)
else()
  option(WITH_SPDK "Enable SPDK" OFF)
endif()
if(WITH_SPDK)
  if(NOT WITH_BLUESTORE)
    message(SEND_ERROR "Please enable WITH_BLUESTORE for using SPDK")
  endif()
  include(BuildSPDK)
  build_spdk()
  set(HAVE_SPDK TRUE)
endif(WITH_SPDK)

option(WITH_PMEM "Enable PMEM" OFF)
if(WITH_PMEM)
  set(HAVE_PMEM ON)
  if(NOT WITH_BLUESTORE)
    message(SEND_ERROR "Please enable WITH_BLUESTORE for using PMEM")
  endif()
endif()

if(WITH_BLUESTORE)
  if(NOT AIO_FOUND AND NOT HAVE_POSIXAIO AND NOT WITH_SPDK AND NOT WITH_PMEM)
    message(SEND_ERROR "WITH_BLUESTORE is ON, "
      "but none of the bluestore backends is enabled. "
      "Please install libaio, or enable WITH_SPDK or WITH_PMEM (experimental)")
  endif()
endif()

option(WITH_BLUEFS "libbluefs library" OFF)

option(WITH_QATZIP "Enable QATZIP" OFF)
if(WITH_QATZIP)
  find_package(qatzip REQUIRED)
  set(HAVE_QATZIP ${QATZIP_FOUND})
endif(WITH_QATZIP)

# needs mds and? XXX
option(WITH_LIBCEPHFS "libcephfs client library" ON)

# key-value store
option(WITH_KVS "Key value store is here" ON)

# remote block storage
option(WITH_RBD "Remote block storage is here" ON)

# KERNEL remote block storage
option(WITH_KRBD "Kernel Remote block storage is here" ON)

if(WITH_KRBD AND WITHOUT_RBD)
  message(FATAL_ERROR "Cannot have WITH_KRBD with WITH_RBD.")
endif()

option(WITH_LEVELDB "LevelDB is here" ON)
if(WITH_LEVELDB)
  if(LEVELDB_PREFIX)
    include_directories(SYSTEM ${LEVELDB_PREFIX}/include)
    link_directories(${LEVELDB_PREFIX}/lib)
  endif()
  find_package(leveldb REQUIRED)
  find_file(HAVE_LEVELDB_FILTER_POLICY leveldb/filter_policy.h PATHS ${LEVELDB_INCLUDE_DIR})
endif(WITH_LEVELDB)

find_package(snappy REQUIRED)

option(WITH_BROTLI "Brotli compression support" OFF)
if(WITH_BROTLI)
  set(HAVE_BROTLI TRUE)
endif()

option(WITH_LZ4 "LZ4 compression support" ON)
if(WITH_LZ4)
  find_package(LZ4 1.7 REQUIRED)
  set(HAVE_LZ4 ${LZ4_FOUND})
endif(WITH_LZ4)

option(WITH_CEPH_DEBUG_MUTEX "Use debug ceph::mutex with lockdep" OFF)

#if allocator is set on command line make sure it matches below strings
set(ALLOCATOR "" CACHE STRING
  "specify memory allocator to use. currently tcmalloc, tcmalloc_minimal, \
jemalloc, and libc is supported. if not specified, will try to find tcmalloc, \
and then jemalloc. If neither of then is found. use the one in libc.")
if(ALLOCATOR)
  if(${ALLOCATOR} MATCHES "tcmalloc(_minimal)?")
    find_package(gperftools REQUIRED)
    set(HAVE_LIBTCMALLOC ON)
  elseif(${ALLOCATOR} STREQUAL "jemalloc")
    find_package(JeMalloc REQUIRED)
    set(HAVE_JEMALLOC 1)
  elseif(NOT ALLOCATOR STREQUAL "libc")
    message(FATAL_ERROR "Unsupported allocator selected: ${ALLOCATOR}")
  endif()
else(ALLOCATOR)
  find_package(gperftools)
  set(HAVE_LIBTCMALLOC ${gperftools_FOUND})
  if(NOT gperftools_FOUND)
    find_package(JeMalloc)
  endif()
  if(gperftools_FOUND)
    set(ALLOCATOR tcmalloc)
  elseif(JeMalloc_FOUND)
    set(ALLOCATOR jemalloc)
  else()
    if(NOT FREEBSD)
      # FreeBSD already has jemalloc as its default allocator
      message(WARNING "tcmalloc and jemalloc not found, falling back to libc")
    endif()
    set(ALLOCATOR "libc")
  endif(gperftools_FOUND)
endif(ALLOCATOR)

if(HAVE_LIBTCMALLOC AND WITH_STATIC_LIBSTDCXX)
  set(EXE_LINKER_USE_PIE FALSE)
else()
  set(EXE_LINKER_USE_PIE ${ENABLE_SHARED})
endif()

if(WITH_LIBCEPHFS OR WITH_KRBD)
  find_package(keyutils REQUIRED)
endif()

find_package(CURL REQUIRED)
set(CMAKE_REQUIRED_INCLUDES ${CURL_INCLUDE_DIRS})
set(CMAKE_REQUIRED_LIBRARIES ${CURL_LIBRARIES})
CHECK_SYMBOL_EXISTS(curl_multi_wait curl/curl.h HAVE_CURL_MULTI_WAIT)

find_package(NSS REQUIRED)
find_package(NSPR REQUIRED)
find_package(OpenSSL REQUIRED)
# TODO: use NSS only for validation of the OpenSSL-based implementations
set(USE_NSS 1)
set(USE_OPENSSL 1)
set(CRYPTO_LIBS ${NSS_LIBRARIES} ${NSPR_LIBRARIES} OpenSSL::Crypto)

option(WITH_DPDK "Enable DPDK messaging" OFF)
if(WITH_DPDK)
  find_package(dpdk)
  if(NOT DPDK_FOUND AND NOT TARGET dpdk-ext)
    include(BuildDPDK)
    build_dpdk(${CMAKE_BINARY_DIR}/src/dpdk)
  endif()
  set(HAVE_DPDK TRUE)
endif()

option(WITH_BLKIN "Use blkin to emit LTTng tracepoints for Zipkin" OFF)
if(WITH_BLKIN)
  set(BLKIN_LIBRARIES blkin ${LTTNGUST_LIBRARIES} lttng-ust-fork)
  include_directories(SYSTEM src/blkin/blkin-lib)
endif(WITH_BLKIN)

option(WITH_BOOST_CONTEXT "Enable boost::asio stackful coroutines" ON)
if(WITH_BOOST_CONTEXT)
  set(HAVE_BOOST_CONTEXT ON)
endif()

#option for RGW
option(WITH_RADOSGW "Rados Gateway is enabled" ON)
option(WITH_RADOSGW_FCGI_FRONTEND "Rados Gateway's FCGI frontend is enabled" OFF)
option(WITH_RADOSGW_BEAST_FRONTEND "Rados Gateway's Beast frontend is enabled" ON)
option(WITH_RADOSGW_BEAST_OPENSSL "Rados Gateway's Beast frontend uses OpenSSL" ON)
option(WITH_RADOSGW_AMQP_ENDPOINT "Rados Gateway's pubsub support for AMQP push endpoint" ON)

if(WITH_RADOSGW)
  find_package(EXPAT REQUIRED)
  if(WITH_RADOSGW_FCGI_FRONTEND)
    find_package(fcgi REQUIRED)
  endif()
  if(WITH_RADOSGW_BEAST_FRONTEND AND NOT WITH_BOOST_CONTEXT)
    message(WARNING "disabling WITH_RADOSGW_BEAST_FRONTEND, which depends on WITH_BOOST_CONTEXT")
    set(WITH_RADOSGW_BEAST_FRONTEND OFF)
  endif()
  find_package(OATH REQUIRED)

# https://curl.haxx.se/docs/install.html mentions the
# configure flags for various ssl backends
  execute_process(
    COMMAND
  "sh" "-c"
  "curl-config --configure | grep with-ssl"
  RESULT_VARIABLE NO_CURL_SSL_LINK
  ERROR_VARIABLE CURL_CONFIG_ERRORS
  )
  if (CURL_CONFIG_ERRORS)
    message(WARNING "unable to run curl-config; rgw cannot make ssl requests to external systems reliably")
  endif()

  if (WITH_RADOSGW_BEAST_FRONTEND AND WITH_RADOSGW_BEAST_OPENSSL)
    find_package(OpenSSL REQUIRED)
  else()
    find_package(OpenSSL)
  endif()

  if (OPENSSL_FOUND)
    if (NOT NO_CURL_SSL_LINK)
      message(STATUS "libcurl is linked with openssl: explicitly setting locks")
      set(WITH_CURL_OPENSSL ON)
    endif() # CURL_SSL_LINK
    execute_process(
      COMMAND
	"sh" "-c"
	"objdump -p ${OPENSSL_SSL_LIBRARY} | sed -n 's/^  SONAME  *//p'"
      OUTPUT_VARIABLE LIBSSL_SONAME
      ERROR_VARIABLE OBJDUMP_ERRORS
      RESULT_VARIABLE OBJDUMP_RESULTS
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    if (OBJDUMP_RESULTS)
      message(FATAL_ERROR "can't run objdump: ${OBJDUMP_RESULTS}")
    endif()
    if (NOT OBJDUMP_ERRORS STREQUAL "")
      message(WARNING "message from objdump: ${OBJDUMP_ERRORS}")
    endif()
    execute_process(
      COMMAND
	"sh" "-c"
	"objdump -p ${OPENSSL_CRYPTO_LIBRARY} | sed -n 's/^  SONAME  *//p'"
      OUTPUT_VARIABLE LIBCRYPTO_SONAME
      ERROR_VARIABLE OBJDUMP_ERRORS
      RESULT_VARIABLE OBJDUMP_RESULTS
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    if (OBJDUMP_RESULTS)
      message(FATAL_ERROR "can't run objdump: ${OBJDUMP_RESULTS}")
    endif()
    if (NOT OBJDUMP_ERRORS STREQUAL "")
      message(WARNING "message from objdump: ${OBJDUMP_ERRORS}")
    endif()
    message(STATUS "ssl soname: ${LIBSSL_SONAME}")
    message(STATUS "crypto soname: ${LIBCRYPTO_SONAME}")
  else()
    message(WARNING "ssl not found: rgw civetweb may fail to dlopen libssl libcrypto")
  endif()  # OPENSSL_FOUND
endif (WITH_RADOSGW)

#option for CephFS
option(WITH_CEPHFS "CephFS is enabled" ON)

#option for Mgr
option(WITH_MGR "ceph-mgr is enabled" ON)
if(WITH_MGR)
  # Please specify 3 or 3.[0-7] if you want to build with python3 support.
  # FindPyhonInterp and FindPythonLibs think they belong to different families.
  set(MGR_PYTHON_VERSION "2.7" CACHE
    STRING "minimal required version of python runtime for running mgr plugins. ")
  find_package(PythonInterp ${MGR_PYTHON_VERSION} REQUIRED)
  find_package(PythonLibs ${MGR_PYTHON_VERSION} REQUIRED)
  set(MGR_PYTHON_EXECUTABLE ${PYTHON_EXECUTABLE})
  set(MGR_PYTHON_LIBRARIES ${PYTHON_LIBRARIES})
  set(MGR_PYTHON_VERSION_MAJOR ${PYTHON_VERSION_MAJOR})
  set(MGR_PYTHON_VERSION_MINOR ${PYTHON_VERSION_MINOR})
  # Boost dependency check deferred to Boost section
endif(WITH_MGR)

option(WITH_THREAD_SAFE_RES_QUERY "res_query is thread safe" OFF)
if(WITH_THREAD_SAFE_RES_QUERY)
  set(HAVE_THREAD_SAFE_RES_QUERY 1 CACHE INTERNAL "Thread safe res_query supported.")
endif()

option(WITH_REENTRANT_STRSIGNAL "strsignal is reentrant" OFF)
if(WITH_REENTRANT_STRSIGNAL)
  set(HAVE_REENTRANT_STRSIGNAL 1 CACHE INTERNAL "Reentrant strsignal is supported.")
endif()

set(HAVE_LIBROCKSDB 1)

# -lz link into kv
find_package(ZLIB REQUIRED)

#option for EventTrace
option(WITH_EVENTTRACE "Event tracing support" OFF)

#option for LTTng
option(WITH_LTTNG "LTTng tracing is enabled" ON)
if(${WITH_LTTNG})
  find_package(LTTngUST REQUIRED)
  find_program(LTTNG_GEN_TP
    lttng-gen-tp)
  if(NOT LTTNG_GEN_TP)
    message(FATAL_ERROR "Can't find lttng-gen-tp.")
  endif()
endif(${WITH_LTTNG})

option(WITH_OSD_INSTRUMENT_FUNCTIONS OFF)

#option for Babeltrace
option(WITH_BABELTRACE "Babeltrace libraries are enabled" ON)
if(WITH_BABELTRACE)
  set(HAVE_BABELTRACE ON)
  find_package(babeltrace REQUIRED)
  set(HAVE_BABELTRACE_BABELTRACE_H ${BABELTRACE_FOUND})
  set(HAVE_BABELTRACE_CTF_EVENTS_H ${BABELTRACE_FOUND})
  set(HAVE_BABELTRACE_CTF_ITERATOR_H ${BABELTRACE_FOUND})
endif(WITH_BABELTRACE)

option(DEBUG_GATHER "C_Gather debugging is enabled" ON)
option(ENABLE_COVERAGE "Coverage is enabled" OFF)
option(PG_DEBUG_REFS "PG Ref debugging is enabled" OFF)

option(WITH_TESTS "enable the build of ceph-test package scripts/binaries" ON)
set(UNIT_TESTS_BUILT ${WITH_TESTS})
set(CEPH_TEST_TIMEOUT 3600 CACHE STRING 
  "Maximum time before a CTest gets killed" )

# fio
option(WITH_FIO "build with fio plugin enabled" OFF)
option(WITH_SYSTEM_FIO "require and build with system fio" OFF)
if(WITH_SYSTEM_FIO)
  find_package(fio REQUIRED)
elseif(WITH_FIO)
  if (NOT FIO_INCLUDE_DIR)
    # Use local external fio if include directory is not set
    set(FIO_INCLUDE_DIR ${CMAKE_BINARY_DIR}/src/fio)
  endif()
  include(BuildFIO)
  build_fio()
endif()

if(LINUX)
  add_definitions(-D__linux__)
endif(LINUX)

# ASAN and friends
option(WITH_ASAN "build with ASAN" OFF)
if(WITH_ASAN)
  list(APPEND sanitizers "address")
endif()

option(WITH_ASAN_LEAK "explicitly enable ASAN leak detection" OFF)
if(WITH_ASAN_LEAK)
  list(APPEND sanitizers "leak")
endif()

option(WITH_TSAN "build with TSAN" OFF)
if(WITH_TSAN)
  list(APPEND sanitizers "thread")
endif()

option(WITH_UBSAN "build with UBSAN" OFF)
if(WITH_UBSAN)
  list(APPEND sanitizers "undefined-behavior")
endif()

if(sanitizers)
  find_package(Sanitizers REQUIRED ${sanitizers})
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${Sanitizers_COMPILE_OPTIONS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${Sanitizers_COMPILE_OPTIONS}")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${Sanitizers_COMPILE_OPTIONS}")
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${Sanitizers_COMPILE_OPTIONS}")
endif()

# Rocksdb
option(WITH_SYSTEM_ROCKSDB "require and build with system rocksdb" OFF)
if (WITH_SYSTEM_ROCKSDB)
  find_package(RocksDB 5.8 REQUIRED)
endif()

option(WITH_SEASTAR "Build seastar components")
set(HAVE_SEASTAR ${WITH_SEASTAR})

# Boost
option(WITH_SYSTEM_BOOST "require and build with system Boost" OFF)

# Boost::thread depends on Boost::atomic, so list it explicitly.
set(BOOST_COMPONENTS
  atomic chrono thread system regex random program_options date_time
  iostreams)
set(BOOST_HEADER_COMPONENTS container)

if(WITH_MGR)
  list(APPEND BOOST_COMPONENTS
    python${MGR_PYTHON_VERSION_MAJOR}${MGR_PYTHON_VERSION_MINOR})
endif()
if(WITH_BOOST_CONTEXT)
  list(APPEND BOOST_COMPONENTS context coroutine)
endif()
if(WITH_SEASTAR)
  list(APPEND BOOST_COMPONENTS filesystem timer)
endif()

set(Boost_USE_MULTITHREADED ON)
# require minimally the bundled version
if(WITH_SYSTEM_BOOST)
  if(ENABLE_SHARED)
    set(Boost_USE_STATIC_LIBS OFF)
  else()
    set(Boost_USE_STATIC_LIBS ON)
  endif()
  if(BOOST_ROOT AND CMAKE_LIBRARY_ARCHITECTURE)
    set(BOOST_LIBRARYDIR "${BOOST_ROOT}/lib/${CMAKE_LIBRARY_ARCHITECTURE}")
  endif()
  find_package(Boost 1.67 COMPONENTS ${BOOST_COMPONENTS} REQUIRED)
else()
  set(BOOST_J 1 CACHE STRING
    "max jobs for Boost build") # override w/-DBOOST_J=<n>
  set(Boost_USE_STATIC_LIBS ON)
  include(BuildBoost)
  build_boost(1.67
    COMPONENTS ${BOOST_COMPONENTS} ${BOOST_HEADER_COMPONENTS})
endif()
include_directories(BEFORE SYSTEM ${Boost_INCLUDE_DIRS})
set(Boost_USE_MULTITHREADED ON)

# dashboard angular2 frontend
option(WITH_MGR_DASHBOARD_FRONTEND "Build the mgr/dashboard frontend using `npm`" ON)
option(WITH_SYSTEM_NPM "Assume that dashboard build tools already installed through packages" OFF)
if(WITH_SYSTEM_NPM)
  find_program(NPM npm)
  if(NOT NPM)
    message(FATAL_ERROR "Can't find npm.")
  endif()
endif()

include_directories(SYSTEM ${PROJECT_BINARY_DIR}/include)

find_package(Threads REQUIRED)
find_package(StdFilesystem)

option(WITH_SELINUX "build SELinux policy" OFF)
if(WITH_SELINUX)
  find_file(SELINUX_MAKEFILE selinux/devel/Makefile
    PATH /usr/share)
  if(NOT SELINUX_MAKEFILE)
    message(FATAL_ERROR "Can't find selinux's Makefile")
  endif()
  add_subdirectory(selinux)
endif(WITH_SELINUX)

# enables testing and creates Make check command
add_custom_target(tests
  COMMENT "Building tests")
enable_testing()
set(CMAKE_CTEST_COMMAND ctest)
add_custom_target(check
  COMMAND ${CMAKE_CTEST_COMMAND}
  DEPENDS tests)

add_subdirectory(src)

add_subdirectory(doc)
if(WITH_MANPAGE)
  add_subdirectory(man)
endif(WITH_MANPAGE)

option(WITH_SYSTEMD "install systemd target and service files" ON)
if(WITH_SYSTEMD)
  add_subdirectory(systemd)
endif()

if(LINUX)
  add_subdirectory(etc/sysctl)
endif()

option(WITH_GRAFANA "install grafana dashboards" OFF)
if(WITH_GRAFANA)
  add_subdirectory(monitoring/grafana/dashboards)
endif()

include(CTags)
option(CTAG_EXCLUDES "Exclude files/directories when running ctag.")
add_tags(ctags
  SRC_DIR src
  TAG_FILE tags
  EXCLUDE_OPTS ${CTAG_EXCLUDES}
  EXCLUDES "*.js" "*.css")
add_custom_target(tags DEPENDS ctags)
