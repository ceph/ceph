/* config.h file expanded by Cmake for build */

#ifndef CONFIG_H
#define CONFIG_H

/* fallocate(2) is supported */
#cmakedefine CEPH_HAVE_FALLOCATE 

/* mallinfo(3) is supported */
#cmakedefine HAVE_MALLINFO

/* posix_fadvise is supported */
#cmakedefine HAVE_POSIX_FADVISE

/* posix_fallocate is supported */
#cmakedefine HAVE_POSIX_FALLOCATE 

/* Define if darwin/osx */
#cmakedefine DARWIN 

/* Define if you want C_Gather debugging */
#cmakedefine DEBUG_GATHER 

/* Define if enabling coverage. */
#cmakedefine ENABLE_COVERAGE

/* FastCGI headers are in /usr/include/fastcgi */
#cmakedefine FASTCGI_INCLUDE_DIR

/* Define to 1 if you have the <arpa/inet.h> header file. */
#cmakedefine HAVE_ARPA_INET_H 1

/* have boost::random::discrete_distribution */
#cmakedefine HAVE_BOOST_RANDOM_DISCRETE_DISTRIBUTION

/* Define to 1 if you have the <dirent.h> header file, and it defines `DIR'.
   */
#cmakedefine HAVE_DIRENT_H 1

/* Define to 1 if you have the <dlfcn.h> header file. */
#cmakedefine HAVE_DLFCN_H 1

/* linux/fiemap.h was found, fiemap ioctl will be used */
#cmakedefine HAVE_FIEMAP_H 1

/* Define to 1 if you have the `fuse_getgroups' function. */
#cmakedefine HAVE_FUSE_GETGROUPS 1

/* Define to 1 if you have the <linux/types.h> header file. */
#cmakedefine HAVE_LINUX_TYPES_H 1

/* Define to 1 if you have the <inttypes.h> header file. */
#cmakedefine HAVE_INTTYPES_H 1

/* Defined if LevelDB supports bloom filters */
#cmakedefine HAVE_LEVELDB_FILTER_POLICY

/* Defined if you don't have atomic_ops */
#cmakedefine HAVE_LIBAIO

/* Define to 1 if you have the `boost_system' library (-lboost_system). */
#cmakedefine HAVE_LIBBOOST_SYSTEM 1

/* Define to 1 if you have the `boost_system-mt' library (-lboost_system-mt).
   */
#cmakedefine HAVE_LIBBOOST_SYSTEM_MT 1

/* Define to 1 if you have the `boost_thread' library (-lboost_thread). */
#cmakedefine HAVE_LIBBOOST_THREAD 1

/* Define to 1 if you have the `boost_thread-mt' library (-lboost_thread-mt).
   */
#cmakedefine HAVE_LIBBOOST_THREAD_MT 1

/* Define if you have fuse */
#cmakedefine HAVE_LIBFUSE

/* Define to 1 if you have the `profiler' library (-lprofiler). */
#cmakedefine HAVE_LIBPROFILER 1

/* Define to 1 if you have the `snappy' library (-lsnappy). */
#cmakedefine HAVE_LIBSNAPPY 1

/* Define if you have jemalloc */
#cmakedefine HAVE_LIBJEMALLOC

/* Define if you have tcmalloc */
#cmakedefine HAVE_LIBTCMALLOC

/* Define if you have tcmalloc */
#cmakedefine HAVE_LIBTCMALLOC_MINIMAL

/* Define to 1 if you have the <memory.h> header file. */
#cmakedefine HAVE_MEMORY_H 1

/* Define to 1 if you have the <ndir.h> header file, and it defines `DIR'. */
#cmakedefine HAVE_NDIR_H 1

/* Define to 1 if you have the <netdb.h> header file. */
#cmakedefine HAVE_NETDB_H 1

/* Define to 1 if you have the <netinet/in.h> header file. */
#cmakedefine HAVE_NETINET_IN_H 1

/* Define if you have perftools profiler enabled */
#cmakedefine HAVE_PROFILER

/* Define if you have POSIX threads libraries and header files. */
#cmakedefine HAVE_PTHREAD

/* Define to 1 if you have the <stdint.h> header file. */
#cmakedefine HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#cmakedefine HAVE_STDLIB_H 1

/* Define to 1 if you have the <strings.h> header file. */
#cmakedefine HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#cmakedefine HAVE_STRING_H 1

/* Define to 1 if you have the `syncfs' function. */
#cmakedefine HAVE_SYNCFS 1

/* Define to 1 if you have the `pwritev' function. */
#cmakedefine HAVE_PWRITEV 1

/* sync_file_range(2) is supported */
#cmakedefine HAVE_SYNC_FILE_RANGE

/* Define to 1 if you have the <syslog.h> header file. */
#cmakedefine HAVE_SYSLOG_H 1

/* Define to 1 if you have the <sys/dir.h> header file, and it defines `DIR'.
   */
#cmakedefine HAVE_SYS_DIR_H 1

/* Define to 1 if you have the <sys/file.h> header file. */
#cmakedefine HAVE_SYS_FILE_H 1

/* Define to 1 if you have the <sys/ioctl.h> header file. */
#cmakedefine HAVE_SYS_IOCTL_H 1

/* Define to 1 if you have the <sys/mount.h> header file. */
#cmakedefine HAVE_SYS_MOUNT_H 1

/* Define to 1 if you have the <sys/ndir.h> header file, and it defines `DIR'.
   */
#cmakedefine HAVE_SYS_NDIR_H 1

/* Define to 1 if you have the <sys/param.h> header file. */
#cmakedefine HAVE_SYS_PARAM_H 1

/* Define to 1 if you have the <sys/socket.h> header file. */
#cmakedefine HAVE_SYS_SOCKET_H 1

/* Define to 1 if you have the <sys/statvfs.h> header file. */
#cmakedefine HAVE_SYS_STATVFS_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#cmakedefine HAVE_SYS_STAT_H 1

/* we have syncfs */
#cmakedefine HAVE_SYS_SYNCFS

/* Define to 1 if you have the <sys/time.h> header file. */
#cmakedefine HAVE_SYS_TIME_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#cmakedefine HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/vfs.h> header file. */
#cmakedefine HAVE_SYS_VFS_H 1

/* Define to 1 if you have <sys/wait.h> that is POSIX.1 compatible. */
#cmakedefine HAVE_SYS_WAIT_H

/* Define to 1 if you have the <sys/xattr.h> header file. */
#cmakedefine HAVE_SYS_XATTR_H

/* Define to 1 if you have the <unistd.h> header file. */
#cmakedefine HAVE_UNISTD_H

/* Define to 1 if you have the <utime.h> header file. */
#cmakedefine HAVE_UTIME_H

/* Define if you have the <execinfo.h> header file. */
#cmakedefine HAVE_EXECINFO_H

/* Define to 1 if strerror_r returns char *. */
#cmakedefine STRERROR_R_CHAR_P 1

/* Define to the sub-directory in which libtool stores uninstalled libraries.
   */
#cmakedefine LT_OBJDIR

/* Defined if you do not have atomic_ops */
#cmakedefine NO_ATOMIC_OPS

/* Define to 1 if your C compiler doesn't accept -c and -o together. */
#cmakedefine NO_MINUS_C_MINUS_O

/* Name of package */
#cmakedefine PACKAGE

/* Define to the address where bug reports for this package should be sent. */
#cmakedefine PACKAGE_BUGREPORT

/* Define to the full name of this package. */
#cmakedefine PACKAGE_NAME

/* Define to the full name and version of this package. */
#cmakedefine PACKAGE_STRING

/* Define to the one symbol short name of this package. */
#cmakedefine PACKAGE_TARNAME

/* Define to the home page for this package. */
#cmakedefine PACKAGE_URL

/* Define to the version of this package. */
#cmakedefine PACKAGE_VERSION

/* Defined if you want pg ref debugging */
#cmakedefine PG_DEBUG_REFS

/* Define to necessary symbol if this constant uses a non-standard name on
   your system. */
#cmakedefine PTHREAD_CREATE_JOINABLE

/* Define to 1 if you have the ANSI C header files. */
#cmakedefine STDC_HEADERS

/* Define if using CryptoPP. */
#cmakedefine USE_CRYPTOPP

/* Define if using NSS. */
#cmakedefine USE_NSS

/* Version number of package */
#cmakedefine VERSION "@VERSION@"

/* define if radosgw enabled */
#cmakedefine WITH_RADOSGW

/* Defined if XIO */
#cmakedefine HAVE_XIO

/* Defined if LTTNG */
#cmakedefine WITH_LTTNG 1

/* Defined if Babeltrace */
#cmakedefine WITH_BABELTRACE 1

/* Define to 1 if you have the <babeltrace/babeltrace.h> header file. */
#cmakedefine HAVE_BABELTRACE_BABELTRACE_H 1

/* Define to 1 if you have the <babeltrace/ctf/events.h> header file. */
#cmakedefine HAVE_BABELTRACE_CTF_EVENTS_H 1

/* Defined if you have librocksdb enabled */
#cmakedefine HAVE_LIBROCKSDB

/* Defined if new gperftools */
#cmakedefine HAVE_GPERFTOOLS_HEAP_PROFILER_H
#cmakedefine HAVE_GPERFTOOLS_MALLOC_EXTENSION_H
#cmakedefine HAVE_GPERFTOOLS_PROFILER_H

/* res_nquery is supported */
#cmakedefine HAVE_RES_NQUERY

/* res_query is thread safe */
#cmakedefine HAVE_THREAD_SAFE_RES_QUERY

/* Define if HAVE_REENTRANT_STRSIGNAL */
#cmakedefine HAVE_REENTRANT_STRSIGNAL

/* Defined if curl headers define curl_multi_wait() */
#cmakedefine HAVE_CURL_MULTI_WAIT 1

/* Define if you have spdk */
#cmakedefine HAVE_SPDK 1

#endif /* CONFIG_H */
