cmake_minimum_required(VERSION 3.10.2)
# remove cmake/modules/FindPython* once 3.12 is required

project(ceph
  VERSION 16.0.0
  LANGUAGES CXX C ASM)

foreach(policy
    CMP0028
    CMP0046
    CMP0048
    CMP0051
    CMP0054
    CMP0056
    CMP0065
    CMP0075)
  if(POLICY ${policy})
    cmake_policy(SET ${policy} NEW)
  endif()
endforeach()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules/")

if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  set(LINUX ON)
  FIND_PACKAGE(Threads)
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  set(FREEBSD ON)
  FIND_PACKAGE(Threads)
endif(CMAKE_SYSTEM_NAME MATCHES "Linux")

if(WIN32)
  # The Windows headers (e.g. coming from mingw or the Windows SDK) check
  # the targeted Windows version. The availability of certain functions and
  # structures will depend on it.
  set(WIN32_WINNT "0x0A00" CACHE STRING "Targeted Windows version.")
  add_definitions(-D_WIN32_WINNT=${WIN32_WINNT})
endif()

if(MINGW)
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,-allow-multiple-definition")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-allow-multiple-definition")
endif()

option(WITH_CCACHE "Build with ccache.")
if(WITH_CCACHE)
  find_program(CCACHE_FOUND ccache)
  if(NOT CCACHE_FOUND)
    message(FATAL_ERROR "Can't find ccache. Is it installed?")
  endif()
  message(STATUS "Building with ccache: ${CCACHE_FOUND}, CCACHE_DIR=$ENV{CCACHE_DIR}")
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  # ccache does not accelerate link (ld), but let it handle it. by passing it
  # along with cc to python's distutils, we are able to workaround
  # https://bugs.python.org/issue8027.
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
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

if(WIN32)
  include_directories(
  ${PROJECT_SOURCE_DIR}/src/include/win32)
  # Boost complains if winsock2.h (or windows.h) is included before asio.hpp.
  add_definitions(-include winsock_wrapper.h)
endif()

if(OFED_PREFIX)
  include_directories(SYSTEM ${OFED_PREFIX}/include)
  link_directories(${OFED_PREFIX}/lib)
endif()

if(FREEBSD)
  include_directories(SYSTEM /usr/local/include)
  link_directories(/usr/local/lib)
  list(APPEND CMAKE_REQUIRED_INCLUDES /usr/local/include)
endif(FREEBSD)


#put all the libs and binaries in one place
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

include(GNUInstallDirs)
include(CephChecks)

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
  if(NOT CMAKE_COMPILER_IS_GNUCXX)
    message(FATAL_ERROR "Please use GCC to enable WITH_STATIC_LIBSTDCXX")
  endif()
  set(static_linker_flags "-static-libstdc++ -static-libgcc")
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${static_linker_flags}")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${static_linker_flags}")
  unset(static_linker_flags)
  set(GPERFTOOLS_USE_STATIC_LIBS TRUE)
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

option(WITH_RBD "Enable RADOS Block Device related targets" ON)

if(LINUX)
  find_package(udev REQUIRED)
  set(HAVE_UDEV ${UDEV_FOUND})
  find_package(blkid REQUIRED)
  set(HAVE_BLKID ${BLKID_FOUND})
  find_package(keyutils REQUIRED)
  set(HAVE_KEYUTILS ${KEYUTILS_FOUND})
elseif(FREEBSD)
  set(HAVE_UDEV OFF)
  set(HAVE_LIBAIO OFF)
  set(HAVE_BLKID OFF)
  set(HAVE_KEYUTILS OFF)
else()
  set(HAVE_UDEV OFF)
  set(HAVE_BLKID OFF)
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
  find_package(FUSE)
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
    find_package(zbc)
    set(HAVE_LIBZBC ${ZBC_FOUND})
  elseif(FREEBSD)
    # POSIX AIO is integrated into FreeBSD kernel, and exposed by libc.
    set(HAVE_POSIXAIO ON)
  endif()
endif()

include(CMakeDependentOption)
CMAKE_DEPENDENT_OPTION(WITH_LIBURING "Enable io_uring bluestore backend" OFF
  "WITH_BLUESTORE;HAVE_LIBAIO" OFF)
set(HAVE_LIBURING ${WITH_LIBURING})

CMAKE_DEPENDENT_OPTION(WITH_SYSTEM_LIBURING "Require and build with system liburing" OFF
  "HAVE_LIBAIO;WITH_BLUESTORE" OFF)

CMAKE_DEPENDENT_OPTION(WITH_BLUESTORE_PMEM "Enable PMDK libraries" OFF
  "WITH_BLUESTORE" OFF)

CMAKE_DEPENDENT_OPTION(WITH_RBD_RWL "Enable librbd persistent write back cache" OFF
  "WITH_RBD" OFF)

CMAKE_DEPENDENT_OPTION(WITH_SYSTEM_PMDK "Require and build with system PMDK" OFF
  "WITH_RBD_RWL OR WITH_BLUESTORE_PMEM" OFF)

if(WITH_BLUESTORE_PMEM)
  set(HAVE_BLUESTORE_PMEM ON)
endif()

CMAKE_DEPENDENT_OPTION(WITH_SPDK "Enable SPDK" OFF
  "CMAKE_SYSTEM_PROCESSOR MATCHES i386|i686|amd64|x86_64|AMD64|aarch64" OFF)
if(WITH_SPDK)
  if(NOT WITH_BLUESTORE)
    message(SEND_ERROR "Please enable WITH_BLUESTORE for using SPDK")
  endif()
  include(BuildSPDK)
  build_spdk()
  set(HAVE_SPDK TRUE)
endif(WITH_SPDK)

if(WITH_BLUESTORE)
  if(NOT AIO_FOUND AND NOT HAVE_POSIXAIO AND NOT WITH_SPDK AND NOT WITH_BLUESTORE_PMEM)
    message(SEND_ERROR "WITH_BLUESTORE is ON, "
      "but none of the bluestore backends is enabled. "
      "Please install libaio, or enable WITH_SPDK or WITH_BLUESTORE_PMEM (experimental)")
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

option(WITH_KRBD "Enable Linux krbd support of 'rbd' utility" ON)

if(WITH_KRBD AND NOT WITH_RBD)
  message(FATAL_ERROR "Cannot have WITH_KRBD without WITH_RBD.")
endif()
if(LINUX)
  if(WITH_LIBCEPHFS OR WITH_KRBD)
    # keyutils is only used when talking to the Linux Kernel key store 
    find_package(keyutils REQUIRED)
    set(HAVE_KEYUTILS ${KEYUTILS_FOUND})
  endif()
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

if (HAVE_LIBTCMALLOC AND TCMALLOC_VERSION_STRING VERSION_LESS 2.6.2)
  set(LIBTCMALLOC_MISSING_ALIGNED_ALLOC ON)
endif()

find_package(CURL REQUIRED)
set(CMAKE_REQUIRED_INCLUDES ${CURL_INCLUDE_DIRS})
set(CMAKE_REQUIRED_LIBRARIES ${CURL_LIBRARIES})
CHECK_SYMBOL_EXISTS(curl_multi_wait curl/curl.h HAVE_CURL_MULTI_WAIT)

find_package(OpenSSL REQUIRED)
set(CRYPTO_LIBS OpenSSL::Crypto)

option(WITH_DPDK "Enable DPDK messaging" OFF)
if(WITH_DPDK)
  find_package(dpdk)
  if(NOT DPDK_FOUND)
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
option(WITH_RADOSGW_KAFKA_ENDPOINT "Rados Gateway's pubsub support for Kafka push endpoint" ON)

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
endif (WITH_RADOSGW)

#option for CephFS
option(WITH_CEPHFS "CephFS is enabled" ON)

# Please specify 3.[0-7] if you want to build with a certain version of python3.
set(WITH_PYTHON3 "3" CACHE STRING "build with specified python3 version")
if(NOT WITH_PYTHON3)
  message(FATAL_ERROR "WITH_PYTHON3 should always be enabled")
elseif(WITH_PYTHON3 MATCHES "^(1|ON|YES|TRUE|Y)$")
  set(WITH_PYTHON3 "3")
  message(NOTICE "Please specify a Python3 version instead of a BOOLEAN")
elseif(NOT WITH_PYTHON3 STREQUAL "3")
  set(find_python3_exact "EXACT")
endif()
find_package(Python3 ${WITH_PYTHON3} ${find_python3_exact} REQUIRED
  COMPONENTS Interpreter Development)
unset(find_python3_exact)

option(WITH_MGR "ceph-mgr is enabled" ON)
if(WITH_MGR)
  set(MGR_PYTHON_EXECUTABLE ${Python3_EXECUTABLE})
  set(MGR_PYTHON_LIBRARIES ${Python3_LIBRARIES})
  set(MGR_PYTHON_VERSION_MAJOR ${Python3_VERSION_MAJOR})
  set(MGR_PYTHON_VERSION_MINOR ${Python3_VERSION_MINOR})
  # https://tracker.ceph.com/issues/45147
  if(Python3_VERSION VERSION_GREATER_EQUAL 3.8)
    set(MGR_DISABLED_MODULES "diskprediction_local")
    message(STATUS "mgr module disabled for ${Python3_VERSION}: ${MGR_DISABLED_MODULES}")
  endif()
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
CMAKE_DEPENDENT_OPTION(
  WITH_EVENTTRACE "Event tracing support, requires WITH_LTTNG"
  OFF "USE_LTTNG" OFF)

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
  list(APPEND sanitizers "undefined_behavior")
endif()

if(sanitizers)
  find_package(Sanitizers REQUIRED ${sanitizers})
  add_compile_options(${Sanitizers_COMPILE_OPTIONS})
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${Sanitizers_COMPILE_OPTIONS}")
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${Sanitizers_COMPILE_OPTIONS}")
endif()

# Rocksdb
option(WITH_SYSTEM_ROCKSDB "require and build with system rocksdb" OFF)
if (WITH_SYSTEM_ROCKSDB)
  find_package(RocksDB 5.14 REQUIRED)
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

# dashboard angular2 frontend
option(WITH_MGR_DASHBOARD_FRONTEND "Build the mgr/dashboard frontend using `npm`" ON)
option(WITH_SYSTEM_NPM "Assume that dashboard build tools already installed through packages" OFF)
if(WITH_SYSTEM_NPM)
  find_program(NPM npm)
  if(NOT NPM)
    message(FATAL_ERROR "Can't find npm.")
  endif()
endif()
set(DASHBOARD_FRONTEND_LANGS "" CACHE STRING
  "List of comma separated ceph-dashboard frontend languages to build. \
  Use value `ALL` to build all languages")

# TODO: make this an option and set it to the same value as WITH_MGR_DASHBOARD_FRONTEND
set(WITH_MGR_ROOK_CLIENT WITH_MGR_DASHBOARD_FRONTEND)

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

add_subdirectory(qa)

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

CMAKE_DEPENDENT_OPTION(WITH_BOOST_VALGRIND "Boost support for valgrind" OFF
  "NOT WITH_SYSTEM_BOOST" OFF)

include(CTags)
option(CTAG_EXCLUDES "Exclude files/directories when running ctag.")
add_tags(ctags
  SRC_DIR src
  TAG_FILE tags
  EXCLUDE_OPTS ${CTAG_EXCLUDES}
  EXCLUDES "*.js" "*.css")
add_custom_target(tags DEPENDS ctags)

set(VERSION 15.2.0)
