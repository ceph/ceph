cmake_minimum_required(VERSION 2.8.11)

project(ceph)
set(VERSION "10.2.1")

if (NOT (CMAKE_MAJOR_VERSION LESS 3))
  # Tweak policies (this one disables "missing" dependency warning)
  cmake_policy(SET CMP0046 OLD)
endif(NOT (CMAKE_MAJOR_VERSION LESS 3))
# we use LINK_PRIVATE keyword instead of PRIVATE, but do not specify the LINK_PUBLIC
# for target_link_libraries() command when PUBLIC should be used instead, it's just
# for backward compatibility with cmake 2.8.11.
if (POLICY CMP0022)
cmake_policy(SET CMP0022 OLD)
endif (POLICY CMP0022)
if (POLICY CMP0023)
cmake_policy(SET CMP0023 OLD)
endif (POLICY CMP0023)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

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
    sphinx-build)
  if(NOT SPHINX_BUILD)
    message(FATAL_ERROR "Can't find sphinx-build.")
  endif(NOT SPHINX_BUILD)
endif(WITH_MANPAGE)

include_directories(
  ${PROJECT_BINARY_DIR}/src/include
  ${OFED_PREFIX}/include
  ${LEVELDB_PREFIX}/include
  ${PROJECT_SOURCE_DIR}/src
)

link_directories(
  ${OFED_PREFIX}/lib
  ${LEVELDB_PREFIX}/lib
)

#put all the libs and binaries in one place
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

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
CHECK_FUNCTION_EXISTS(mallinfo HAVE_MALLINFO)
CHECK_FUNCTION_EXISTS(pwritev HAVE_PWRITEV)
CHECK_FUNCTION_EXISTS(splice CEPH_HAVE_SPLICE)
CHECK_FUNCTION_EXISTS(getgrouplist HAVE_GETGROUPLIST)
CHECK_FUNCTION_EXISTS(fdatasync HAVE_FDATASYNC)
CHECK_FUNCTION_EXISTS(strerror_r HAVE_STRERROR_R)
CHECK_FUNCTION_EXISTS(name_to_handle_at HAVE_NAME_TO_HANDLE_AT)
CHECK_FUNCTION_EXISTS(pipe2 HAVE_PIPE2)
set(CMAKE_REQUIRED_LIBRARIES pthread)
CHECK_FUNCTION_EXISTS(pthread_spin_init HAVE_PTHREAD_SPINLOCK)
CHECK_FUNCTION_EXISTS(eventfd HAVE_EVENTFD)

CHECK_INCLUDE_FILE_CXX("boost/asio/coroutine.hpp" HAVE_BOOST_ASIO_COROUTINE)
CHECK_INCLUDE_FILES("inttypes.h" HAVE_INTTYPES_H)
CHECK_INCLUDE_FILES("linux/types.h" HAVE_LINUX_TYPES_H)
CHECK_INCLUDE_FILES("linux/version.h" HAVE_LINUX_VERSION_H)
CHECK_INCLUDE_FILES("stdint.h" HAVE_STDINT_H)
CHECK_INCLUDE_FILES("arpa/nameser_compat.h" HAVE_ARPA_NAMESER_COMPAT_H)
CHECK_INCLUDE_FILES("sys/mount.h" HAVE_SYS_MOUNT_H)
CHECK_INCLUDE_FILES("sys/param.h" HAVE_SYS_PARAM_H)
CHECK_INCLUDE_FILES("sys/types.h" HAVE_SYS_TYPES_H)
CHECK_INCLUDE_FILES("sys/vfs.h" HAVE_SYS_VFS_H)
CHECK_INCLUDE_FILES("sys/prctl.h" HAVE_SYS_PRCTL_H)
CHECK_INCLUDE_FILES("execinfo.h" HAVE_EXECINFO_H)
CHECK_INCLUDE_FILES("sched.h" HAVE_SCHED)
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

include(CheckCXXSourceCompiles)
CHECK_CXX_SOURCE_COMPILES("
  #include <string.h>
  int main() { char x = *strerror_r(0, &x, sizeof(x)); return 0; }
  " STRERROR_R_CHAR_P)
CHECK_CXX_SOURCE_COMPILES("
  #include <sys/stat.h>
  #include <stdio.h>
  int main() { struct stat foo = {}; foo.st_mtim.tv_nsec = 0;
  return 0; } 
  " HAVE_STAT_ST_MTIM_TV_NSEC)
CHECK_CXX_SOURCE_COMPILES("
  #include <sys/stat.h>
  #include <stdio.h>
  int main() { struct stat foo = {}; foo.st_mtimespec.tv_nsec = 0;
  return 0; } 
  " HAVE_STAT_ST_MTIMESPEC_TV_NSEC)
CHECK_CXX_SOURCE_COMPILES("
  int main() { float f = 12.3; int n = static_cast<int>(f); return 0; }
  " HAVE_STATIC_CAST)

set(CEPH_MAN_DIR "share/man" CACHE STRING "Install location for man pages (relative to prefix).")

option(ENABLE_SHARED "build shared libraries" ON)
if(ENABLE_SHARED)
  set(CEPH_SHARED SHARED)
else(ENABLE_SHARED)
  set(CEPH_SHARED STATIC)
endif(ENABLE_SHARED)

find_package(execinfo)

find_package(udev REQUIRED)
set(HAVE_UDEV ${UDEV_FOUND})

option(WITH_AIO "AIO is here ON" ON)
if(${WITH_AIO})
find_package(aio REQUIRED)
set(HAVE_LIBAIO ${AIO_FOUND})
message(STATUS "${AIO_LIBS}")
endif(${WITH_AIO})

option(WITH_OPENLDAP "OPENLDAP is here" ON)
if(${WITH_OPENLDAP})
find_package(OpenLdap REQUIRED)
set(HAVE_OPENLDAP ${OPENLDAP_FOUND})
message(STATUS "${OPENLDAP_LIBS}")
endif(${WITH_OPENLDAP})

option(WITH_OPENSSL "OPENSSL is here" ON)
if(${WITH_OPENSSL})
find_package(OpenSSL REQUIRED)
set(HAVE_OPENSSL ON)
#message(STATUS "${OPENSSL_LIBRARIES}")
else(${WITH_OPENSSL})
set(HAVE_OPENSSL OFF)
set(OPENSSL_LIBRARIES)
endif(${WITH_OPENSSL})

option(WITH_FUSE "Fuse is here" ON)
if(${WITH_FUSE})
find_package(fuse)
set(HAVE_LIBFUSE ${FUSE_FOUND})
endif(${WITH_FUSE})

option(WITH_XFS "XFS is here" ON)
if(${WITH_XFS})
find_package(xfs)
set(HAVE_LIBXFS ${XFS_FOUND})
endif(${WITH_XFS})

option(WITH_SPDK "Enable SPDK" OFF)
if(WITH_SPDK)
  find_package(pciaccess REQUIRED)
  find_package(dpdk REQUIRED)
  set(HAVE_SPDK TRUE)
endif(WITH_SPDK)

find_package(blkid REQUIRED)

# probably fuse etc need this -- how to check? XXX
option(WITH_MDS "MDS is here" ON)

# needs mds and? XXX
option(WITH_LIBCEPHFS "libcephfs client library" ON)

# key-value store
option(WITH_KVS "Key value store is here" ON)

# remote block storage
option(WITH_RBD "Remote block storage is here" ON)

option(WITH_LEVELDB "LevelDB is here" ON)
if(${WITH_LEVELDB})
find_package(leveldb REQUIRED)
find_file(HAVE_LEVELDB_FILTER_POLICY filter_policy.h PATHS /usr/include/leveldb)
endif(${WITH_LEVELDB})

option(WITH_EXPAT "EXPAT is here" ON)
if(${WITH_EXPAT})
find_package(expat REQUIRED)
endif(${WITH_EXPAT})

find_package(Cython REQUIRED)
find_package(fcgi REQUIRED)

find_package(atomic_ops REQUIRED)
message(STATUS "${ATOMIC_OPS_LIBRARIES}")
if(NOT ${ATOMIC_OPS_FOUND})
  set(NO_ATOMIC_OPS 1)
endif(NOT ${ATOMIC_OPS_FOUND})

option(WITH_GPERFTOOLS "gperftools is here" ON)
if(${WITH_GPERFTOOLS})
	find_package(gperftools)
	set(HAVE_GPERFTOOLS ${GPERFTOOLS_FOUND})
	if(${HAVE_GPERFTOOLS})
		find_file(HAVE_GPERFTOOLS_HEAP_PROFILER_H heap-profiler.h PATHS /usr/include/gperftools)
		find_file(HAVE_GPERFTOOLS_MALLOC_EXTENSION_H malloc_extension.h PATHS /usr/include/gperftools)
		find_file(HAVE_GPERFTOOLS_PROFILER_H profiler.h PATHS /usr/include/gperftools)
	endif(${HAVE_GPERFTOOLS})
endif(${WITH_GPERFTOOLS})

find_package(snappy REQUIRED)

#if allocator is set on command line make sure it matches below strings
if(ALLOCATOR)
  if(${ALLOCATOR} STREQUAL "tcmalloc")
    find_package(tcmalloc REQUIRED)
    set(HAVE_LIBTCMALLOC ${Tcmalloc_FOUND})
  elseif(${ALLOCATOR} STREQUAL "jemalloc")
    find_package(JeMalloc REQUIRED)
    set(HAVE_LIBJEMALLOC ${JEMALLOC_FOUND})
  endif()
else(ALLOCATOR)
  find_package(tcmalloc)
  set(HAVE_LIBTCMALLOC ${Tcmalloc_FOUND})
  if(NOT ${Tcmalloc_FOUND})
    find_package(JeMalloc)
    set(HAVE_LIBJEMALLOC ${JEMALLOC_FOUND})
  endif(NOT ${Tcmalloc_FOUND})
  if(Tcmalloc_FOUND)
    set(ALLOCATOR tcmalloc)
  elseif(JEMALLOC_FOUND)
    set(ALLOCATOR jemalloc)
  else()
    message(WARNING "tcmalloc and jemalloc not found, falling back to libc")
    set(ALLOCATOR "libc")
  endif(Tcmalloc_FOUND)
endif(ALLOCATOR)

find_package(keyutils REQUIRED)

find_package(libuuid REQUIRED)

find_package(libcurl REQUIRED)

option(USE_CRYPTOPP "Cryptopp is ON" ON)
find_package(cryptopp)
if(CRYPTOPP_FOUND)
        MESSAGE(STATUS "${CRYPTOPP_LIBRARIES}")
        set(CRYPTO_LIBS ${CRYPTOPP_LIBRARIES})
	set(USE_NSS 0)
else()
        MESSAGE(STATUS "Cryptopp not found using NSS instead")
	find_package(NSS REQUIRED)
        if(NSS_FOUND)
		set(USE_NSS 1)
		set(USE_CRYPTOPP 0)
		find_package(NSPR)
		if(NSPR_FOUND)
			set(CRYPTO_LIBS ${NSS_LIBRARIES} ${NSPR_LIBRARIES})
			#MESSAGE(STATUS "${CRYPTO_LIBS}")
			#MESSAGE(STATUS "${NSS_INCLUDE_DIR} ${NSPR_INCLUDE_DIR}")
		endif(NSPR_FOUND)
	endif(NSS_FOUND)
endif(CRYPTOPP_FOUND)

option(WITH_XIO "Enable XIO messaging" OFF)
if(WITH_XIO)
  find_package(xio REQUIRED)
set(HAVE_XIO ${XIO_FOUND})
endif(WITH_XIO)

#option for RGW
option(WITH_RADOSGW "Rados Gateway is enabled" ON)

#option for CephFS
option(WITH_CEPHFS "CephFS is enabled" ON)

option(WITH_THREAD_SAFE_RES_QUERY "res_query is thread safe" OFF)
if(WITH_THREAD_SAFE_RES_QUERY)
    set(HAVE_THREAD_SAFE_RES_QUERY 1 CACHE INTERNAL "Thread safe res_query supported.")
endif(WITH_THREAD_SAFE_RES_QUERY)

option(WITH_REENTRANT_STRSIGNAL "strsignal is reentrant" OFF)
if(WITH_REENTRANT_STRSIGNAL)
    set(HAVE_REENTRANT_STRSIGNAL 1 CACHE INTERNAL "Reentrant strsignal is supported.")
endif(WITH_REENTRANT_STRSIGNAL)

set(HAVE_LIBROCKSDB 1)

# -lz link into kv
find_package(ZLIB REQUIRED)

#option for LTTng
#currently off by default because lttng-gen-tp run unconditionally and forces a rebuild
option(WITH_LTTNG "LTTng tracing is enabled" OFF)
if(${WITH_LTTNG})
  find_package(lttng-ust REQUIRED)
endif(${WITH_LTTNG})

#option for Babeltrace
option(HAVE_BABELTRACE "Babeltrace libraries are enabled" ON)
if(${HAVE_BABELTRACE})
  find_package(babeltrace REQUIRED)
  set(WITH_BABELTRACE ${BABELTRACE_FOUND})
  set(HAVE_BABELTRACE_BABELTRACE_H ${BABELTRACE_FOUND})
  set(HAVE_BABELTRACE_CTF_EVENTS_H ${BABELTRACE_FOUND})
  set(HAVE_BABELTRACE_CTF_ITERATOR_H ${BABELTRACE_FOUND})
endif(${HAVE_BABELTRACE})

option(DEBUG_GATHER "C_Gather debugging is enabled" ON)
option(HAVE_LIBZFS "LibZFS is enabled" OFF)
option(ENABLE_COVERAGE "Coverage is enabled" OFF)
option(PG_DEBUG_REFS "PG Ref debugging is enabled" OFF)

add_definitions(-D__linux__)

if(ENABLE_SHARED)
  set(Boost_USE_STATIC_LIBS   OFF)
else(ENABLE_SHARED)
  set(Boost_USE_STATIC_LIBS   ON)
endif(ENABLE_SHARED)

set(Boost_USE_MULTITHREADED ON)
find_package(Boost COMPONENTS thread system regex random program_options date_time iostreams REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${PROJECT_BINARY_DIR}/include)

find_package(Threads REQUIRED)

option(WITH_SELINUX "build SELinux policy" OFF)
if(WITH_SELINUX)
  find_file(SELINUX_MAKEFILE selinux/devel/include/Makefile
    PATH /usr/share)
  if(NOT SELINUX_MAKEFILE)
    message(FATAL_ERROR "Can't find selinux's Makefile")
  endif()
  add_subdirectory(selinux)
endif(WITH_SELINUX)

# find out which platform we are building on
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(LINUX ON)
  set(UNIX ON)
  FIND_PACKAGE(Threads)
endif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")

# find out which platform we are building on
if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
set(OperatingSystem "Mac OS X")
endif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

# enables testing and creates Make check command
enable_testing()
set(CMAKE_CTEST_COMMAND ctest)
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})

add_subdirectory(src)

add_subdirectory(doc)
if(WITH_MANPAGE)
  add_subdirectory(man)
endif(WITH_MANPAGE)
