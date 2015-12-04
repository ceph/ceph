cmake_minimum_required(VERSION 2.8.11)

project(Ceph)
set(VERSION "0.90")

if (NOT (CMAKE_MAJOR_VERSION LESS 3))
  # Tweak policies (this one disables "missing" dependency warning)
  cmake_policy(SET CMP0046 OLD)
endif(NOT (CMAKE_MAJOR_VERSION LESS 3))

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

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

#Check Includes
include(CheckIncludeFiles)
include(CheckIncludeFileCXX)
include(CheckFunctionExists)

CHECK_FUNCTION_EXISTS(fallocate CEPH_HAVE_FALLOCATE)
CHECK_FUNCTION_EXISTS(posix_fadvise HAVE_POSIX_FADVISE)
CHECK_FUNCTION_EXISTS(posix_fallocate HAVE_POSIX_FALLOCATE)
CHECK_FUNCTION_EXISTS(syncfs HAVE_SYS_SYNCFS) 
CHECK_FUNCTION_EXISTS(sync_file_range HAVE_SYNC_FILE_RANGE)
CHECK_FUNCTION_EXISTS(mallinfo HAVE_MALLINFO)
CHECK_INCLUDE_FILES("arpa/inet.h" HAVE_ARPA_INET_H)
CHECK_INCLUDE_FILES("boost/random/discrete_distribution.hpp" HAVE_BOOST_RANDOM_DISCRETE_DISTRIBUTION)
CHECK_INCLUDE_FILES("dirent.h" HAVE_DIRENT_H)
CHECK_INCLUDE_FILES("dlfcn.h" HAVE_DLFCN_H)
CHECK_INCLUDE_FILES("inttypes.h" HAVE_INTTYPES_H)
CHECK_INCLUDE_FILES("linux/types.h" HAVE_LINUX_TYPES_H)
CHECK_INCLUDE_FILES("memory.h" HAVE_MEMORY_H)
CHECK_INCLUDE_FILES("ndir.h" HAVE_NDIR_H)
CHECK_INCLUDE_FILES("netdb.h" HAVE_NETDB_H)
CHECK_INCLUDE_FILES("netinet/in.h" HAVE_NETINET_IN_H)
CHECK_INCLUDE_FILES("stdint.h" HAVE_STDINT_H)
CHECK_INCLUDE_FILES("stdlib.h" HAVE_STDLIB_H)
CHECK_INCLUDE_FILES("arpa/inet.h" HAVE_ARPA_INET_H)
CHECK_INCLUDE_FILES("strings.h" HAVE_STRINGS_H)
CHECK_INCLUDE_FILES("string.h" HAVE_STRING_H)
CHECK_INCLUDE_FILES("syslog.h" HAVE_SYSLOG_H)
CHECK_INCLUDE_FILES("sys/dir.h" HAVE_SYS_DIR_H)
CHECK_INCLUDE_FILES("sys/file.h" HAVE_SYS_FILE_H)
CHECK_INCLUDE_FILES("sys/ioctl.h" HAVE_SYS_IOCTL_H)
CHECK_INCLUDE_FILES("sys/mount.h" HAVE_SYS_MOUNT_H)
CHECK_INCLUDE_FILES("sys/ndir.h" HAVE_SYS_NDIR_H)
CHECK_INCLUDE_FILES("sys/param.h" HAVE_SYS_PARAM_H)
CHECK_INCLUDE_FILES("sys/socket.h" HAVE_SYS_SOCKET_H)
CHECK_INCLUDE_FILES("sys/statvfs.h" HAVE_SYS_STATVFS_H)
CHECK_INCLUDE_FILES("sys/stat.h" HAVE_SYS_STAT_H)
CHECK_INCLUDE_FILES("sys/time.h" HAVE_SYS_TIME_H)
CHECK_INCLUDE_FILES("sys/types.h" HAVE_SYS_TYPES_H)
CHECK_INCLUDE_FILES("sys/vfs.h" HAVE_SYS_VFS_H)
CHECK_INCLUDE_FILES("sys/wait.h" HAVE_SYS_WAIT_H)
CHECK_INCLUDE_FILES("sys/xattr.h" HAVE_SYS_XATTR_H)
CHECK_INCLUDE_FILES("unistd.h" HAVE_UNISTD_H)
CHECK_INCLUDE_FILES("utime.h" HAVE_UTIME_H)
CHECK_INCLUDE_FILES("${CMAKE_SOURCE_DIR}/src/include/fiemap.h" HAVE_FIEMAP_H)
CHECK_INCLUDE_FILES("fuse/fuse_lowlevel.h" HAVE_FUSE_LOWLEVEL_H)
CHECK_INCLUDE_FILES("fuse/fuse.h" HAVE_FUSE_H)
CHECK_INCLUDE_FILES("fcgi_config.h"  HAVE_FASTCGI_CONFIG_H)
CHECK_INCLUDE_FILES("fastcgi.h" HAVE_FASTCGI_H)
CHECK_INCLUDE_FILES("fcgiapp.h" FASTCGI_FASTCGI_APP_DIR)
CHECK_INCLUDE_FILES("fcgimisc.h" HAVE_FASTCGI_MISC_H)
CHECK_INCLUDE_FILES("fcgio.h" HAVE_FASTCGIO_H)
CHECK_INCLUDE_FILES("fcgios.h" FASTCGI_FASTCGIOS_DIR)
CHECK_INCLUDE_FILES("fcgi_stdio.h" HAVE_FASTCGI_STDIO_H)
CHECK_INCLUDE_FILES("openssl/ssl.h" HAVE_SSL_H)
CHECK_INCLUDE_FILES("keyutils.h" HAVE_KEYUTILS_H)

include(CheckSymbolExists)
CHECK_SYMBOL_EXISTS(__u8 "sys/types.h;linux/types.h" HAVE___U8)
CHECK_SYMBOL_EXISTS(__u64 "sys/types.h;linux/types.h" HAVE___U64)
CHECK_SYMBOL_EXISTS(__s64 "sys/types.h;linux/types.h" HAVE___S64)

CHECK_SYMBOL_EXISTS(res_nquery "resolv.h" HAVE_RES_NQUERY)

include(CheckCXXSourceCompiles)
CHECK_CXX_SOURCE_COMPILES("
  #include <string.h>
  int main() { char x = *strerror_r(0, &x, sizeof(x)); return 0; }
  " STRERROR_R_CHAR_P)

set(CEPH_MAN_DIR "share/man" CACHE STRING "Install location for man pages (relative to prefix).")

option(ENABLE_SHARED "build shared libraries" ON)
if(${ENABLE_SHARED})
 set(CEPH_SHARED SHARED)
else(${ENABLE_SHARED})
 set(CEPH_SHARED STATIC)
endif(${ENABLE_SHARED})

find_package(udev REQUIRED)
set(HAVE_UDEV ${UDEV_FOUND})

option(WITH_AIO "AIO is here ON" ON)
if(${WITH_AIO})
find_package(aio REQUIRED)
set(HAVE_LIBAIO ${AIO_FOUND})
message(STATUS "${AIO_LIBS}")
endif(${WITH_AIO})

option(WITH_FUSE "Fuse is here" ON)
if(${WITH_FUSE})
find_package(fuse)
set(HAVE_FUSE ${FUSE_FOUND})
CHECK_FUNCTION_EXISTS(fuse_getgroups HAVE_FUSE_GETGROUPS)
endif(${WITH_FUSE})

option(WITH_XFS "XFS is here" ON)
if(${WITH_XFS})
find_package(xfs)
set(HAVE_XFS ${XFS_FOUND})
endif(${WITH_XFS})

find_package(blkid REQUIRED)
set(HAVE_LIBBLKID ${BLKID_FOUND})

# probably fuse etc need this -- how to check? XXX
option(WITH_MDS "MDS is here" ON)

# needs mds and? XXX
option(WITH_CLIENT "Client is here" ON)
option(WITH_LIBCEPHFS "libcephfs client library" ON)

# key-value store
option(WITH_KVS "Key value store is here" ON)

# remote block storage
option(WITH_RBD "Remote block storage is here" ON)

option(WITH_LEVELDB "LevelDB is here" ON)
if(${WITH_LEVELDB})
find_package(leveldb REQUIRED)
set(HAVE_LIBLEVELDB ${LEVELDB_FOUND})
find_file(HAVE_LEVELDB_FILTER_POLICY filter_policy.h PATHS /usr/include/leveldb)
endif(${WITH_LEVELDB})

option(WITH_EXPAT "EXPAT is here" ON)
if(${WITH_EXPAT})
find_package(expat REQUIRED)
set(HAVE_EXPAT ${EXPAT_FOUND})
endif(${WITH_EXPAT})

find_package(fcgi REQUIRED)

find_package(atomic_ops REQUIRED)
set(HAVE_ATOMIC_OPS ${ATOMIC_OPS_FOUND})
message(STATUS "${ATOMIC_OPS_LIBRARIES}")

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

option(WITH_TCMALLOC "Use TCMalloc as Allocator" ON)
if(${WITH_TCMALLOC})
find_package(tcmalloc REQUIRED)
set(HAVE_LIBTCMALLOC ${Tcmalloc_FOUND})
endif(${WITH_TCMALLOC})

find_package(keyutils REQUIRED)

find_package(libuuid REQUIRED)

find_package(libcurl REQUIRED)

find_package(libedit REQUIRED)

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

option(WITH_XIO "Enable XIO messaging" ON)
if(WITH_XIO)
find_package(xio)
set(HAVE_XIO ${Xio_FOUND})
endif(WITH_XIO)

#option for RGW
option(WITH_RADOSGW "Rados Gateway is enabled" ON)

#option for CephFS
option(WITH_CEPHFS "CephFS is enabled" ON)

option(WITH_THREAD_SAFE_RES_QUERY "res_query is thread safe" OFF)
if(WITH_THREAD_SAFE_RES_QUERY)
    set(HAVE_THREAD_SAFE_RES_QUERY 1 CACHE INTERNAL "Thread safe res_query supported.")
endif(WITH_THREAD_SAFE_RES_QUERY)

# Now create a useable config.h
configure_file(
  ${PROJECT_SOURCE_DIR}/src/include/config-h.in.cmake
  ${PROJECT_BINARY_DIR}/include/acconfig.h
)
include_directories(${PROJECT_BINARY_DIR}/include)

add_definitions(-D__linux__)

if(${ENABLE_SHARED})
  set(Boost_USE_STATIC_LIBS   OFF)
else(${ENABLE_SHARED})
  set(Boost_USE_STATIC_LIBS   ON)
endif(${ENABLE_SHARED})

set(Boost_USE_MULTITHREADED ON)
find_package(Boost COMPONENTS thread system regex random REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

find_package(Threads REQUIRED)

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
set(CMAKE_CTEST_COMMAND ctest -V)
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})

add_subdirectory(src)

# man pages must be preprocessed, not supported yet
#add_subdirectory(man)
