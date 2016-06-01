/* config.h file expanded by Cmake for build */

#ifndef CONFIG_H
#define CONFIG_H

/* fallocate(2) is supported */
#cmakedefine CEPH_HAVE_FALLOCATE

/* Define to 1 if you have the `posix_fadvise' function. */
#cmakedefine HAVE_POSIX_FADVISE 1

/* Define to 1 if you have the `posix_fallocate' function. */
#cmakedefine HAVE_POSIX_FALLOCATE 1

/* Define to 1 if you have the `syncfs' function. */
#cmakedefine HAVE_SYS_SYNCFS 1

/* sync_file_range(2) is supported */
#cmakedefine HAVE_SYNC_FILE_RANGE

/* Define if you have mallinfo */
#cmakedefine HAVE_MALLINFO

/* Define to 1 if you have the `pwritev' function. */
#cmakedefine HAVE_PWRITEV 1

/* Define to 1 if you have the <sys/mount.h> header file. */
#cmakedefine HAVE_SYS_MOUNT_H 1

/* Define to 1 if you have the <sys/param.h> header file. */
#cmakedefine HAVE_SYS_PARAM_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#cmakedefine HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/vfs.h> header file. */
#cmakedefine HAVE_SYS_VFS_H 1

/* Define to 1 if you have the <execinfo.h> header file. */
#cmakedefine HAVE_EXECINFO_H 1

/* Define to 1 if the system has the type `__be16'. */
#cmakedefine HAVE___BE16 1

/* Define to 1 if the system has the type `__be32'. */
#cmakedefine HAVE___BE32 1

/* Define to 1 if the system has the type `__be64'. */
#cmakedefine HAVE___BE64 1

/* Define to 1 if the system has the type `__le16'. */
#cmakedefine HAVE___LE16 1

/* Define to 1 if the system has the type `__le32'. */
#cmakedefine HAVE___LE32 1

/* Define to 1 if the system has the type `__le64'. */
#cmakedefine HAVE___LE64 1

/* Define to 1 if the system has the type `__s16'. */
#cmakedefine HAVE___S16 1

/* Define to 1 if the system has the type `__s32'. */
#cmakedefine HAVE___S32 1

/* Define to 1 if the system has the type `__s64'. */
#cmakedefine HAVE___S64 1

/* Define to 1 if the system has the type `__s8'. */
#cmakedefine HAVE___S8 1

/* Define to 1 if the system has the type `__u16'. */
#cmakedefine HAVE___U16 1

/* Define to 1 if the system has the type `__u32'. */
#cmakedefine HAVE___U32 1

/* Define to 1 if the system has the type `__u64'. */
#cmakedefine HAVE___U64 1

/* Define to 1 if the system has the type `__u8'. */
#cmakedefine HAVE___U8 1

/* Define if you have res_nquery */
#cmakedefine HAVE_RES_NQUERY

/* Defined if you don't have atomic_ops */
#cmakedefine NO_ATOMIC_OPS

/* Defined if you have libaio */
#cmakedefine HAVE_LIBAIO

/* Defined if OpenLDAP enabled */
#cmakedefine HAVE_OPENLDAP

/* Define if you have fuse */
#cmakedefine HAVE_LIBFUSE

/* Define to 1 if you have libxfs */
#cmakedefine HAVE_LIBXFS 1

/* SPDK conditional compilation */
#cmakedefine HAVE_SPDK

/* Defined if LevelDB supports bloom filters */
#cmakedefine HAVE_LEVELDB_FILTER_POLICY

/* Define to 1 if you have the <gperftools/heap-profiler.h> header file. */
#cmakedefine HAVE_GPERFTOOLS_HEAP_PROFILER_H 1

/* Define to 1 if you have the <gperftools/malloc_extension.h> header file. */
#cmakedefine HAVE_GPERFTOOLS_MALLOC_EXTENSION_H 1

/* Define to 1 if you have the <gperftools/profiler.h> header file. */
#cmakedefine HAVE_GPERFTOOLS_PROFILER_H 1

/* Define if you have tcmalloc */
#cmakedefine HAVE_LIBTCMALLOC

/* Define if you have jemalloc */
#cmakedefine HAVE_LIBJEMALLOC

/* Define if have curl_multi_wait() */
#cmakedefine HAVE_CURL_MULTI_WAIT 1

/* Define if using CryptoPP. */
#cmakedefine USE_CRYPTOPP

/* Define if using NSS. */
#cmakedefine USE_NSS

/* Accelio conditional compilation */
#cmakedefine HAVE_XIO

/* define if radosgw enabled */
#cmakedefine WITH_RADOSGW

/* define if HAVE_THREAD_SAFE_RES_QUERY */
#cmakedefine HAVE_THREAD_SAFE_RES_QUERY

/* define if HAVE_REENTRANT_STRSIGNAL */
#cmakedefine HAVE_REENTRANT_STRSIGNAL

/* Define if you want to use LTTng */
#cmakedefine WITH_LTTNG

/* Define if you want to use Babeltrace */
#cmakedefine WITH_BABELTRACE

/* Define to 1 if you have the <babeltrace/babeltrace.h> header file. */
#cmakedefine HAVE_BABELTRACE_BABELTRACE_H 1

/* Define to 1 if you have the <babeltrace/ctf/events.h> header file. */
#cmakedefine HAVE_BABELTRACE_CTF_EVENTS_H 1

/* Define to 1 if you have the <babeltrace/ctf/iterator.h> header file. */
#cmakedefine HAVE_BABELTRACE_CTF_ITERATOR_H 1

/* Define to 1 if you have the <arpa/nameser_compat.h> header file. */
#cmakedefine HAVE_ARPA_NAMESER_COMPAT_H 1

/* FastCGI headers are in /usr/include/fastcgi */
#cmakedefine FASTCGI_INCLUDE_DIR

/* splice(2) is supported */
#cmakedefine CEPH_HAVE_SPLICE

/* Define if you want C_Gather debugging */
#cmakedefine DEBUG_GATHER

/* Define to 1 if you have the `getgrouplist' function. */
#cmakedefine HAVE_GETGROUPLIST 1

/* LTTng is disabled, so define this macro to be nothing. */
#cmakedefine tracepoint

/* have boost::asio::coroutine */
#cmakedefine HAVE_BOOST_ASIO_COROUTINE

/* Define to 1 if you have fdatasync. */
#cmakedefine HAVE_FDATASYNC 1

/* Define to 1 if you have the <inttypes.h> header file. */
#cmakedefine HAVE_INTTYPES_H 1

/* Defined if you have librocksdb enabled */
#cmakedefine HAVE_LIBROCKSDB

/* Define to 1 if you have the <valgrind/helgrind.h> header file. */
#cmakedefine HAVE_VALGRIND_HELGRIND_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#cmakedefine HAVE_STDINT_H 1

/* Define to 1 if you have the <sys/prctl.h> header file. */
#cmakedefine HAVE_SYS_PRCTL_H 1

/* Define to 1 if you have the <linux/types.h> header file. */
#cmakedefine HAVE_LINUX_TYPES_H 1

/* Define to 1 if you have the <linux/version.h> header file. */
#cmakedefine HAVE_LINUX_VERSION_H 1

/* Define to 1 if you have sched.h. */
#cmakedefine HAVE_SCHED 1

/* Support SSE (Streaming SIMD Extensions) instructions */
#cmakedefine HAVE_SSE

/* Support SSE2 (Streaming SIMD Extensions 2) instructions */
#cmakedefine HAVE_SSE2

/* Define to 1 if you have the `pipe2' function. */
#cmakedefine HAVE_PIPE2 1

/* Support NEON instructions */
#cmakedefine HAVE_NEON

/* Define if you have pthread_spin_init */
#cmakedefine HAVE_PTHREAD_SPINLOCK

/* name_to_handle_at exists */
#cmakedefine HAVE_NAME_TO_HANDLE_AT

/* we have a recent yasm and are x86_64 */
#cmakedefine HAVE_GOOD_YASM_ELF64 

/* yasm can also build the isa-l */
#cmakedefine HAVE_BETTER_YASM_ELF64

/* Define to 1 if strerror_r returns char *. */
#cmakedefine STRERROR_R_CHAR_P 1

/* Defined if you have libzfs enabled */
#cmakedefine HAVE_LIBZFS

/* Define if the C complier supports __func__ */
#cmakedefine HAVE_FUNC

/* Define if the C complier supports __PRETTY_FUNCTION__ */
#cmakedefine HAVE_PRETTY_FUNC

/* F_SETPIPE_SZ is supported */
#cmakedefine CEPH_HAVE_SETPIPE_SZ

/* Have eventfd extension. */
#cmakedefine HAVE_EVENTFD

/* Define if enabling coverage. */
#cmakedefine ENABLE_COVERAGE

/* Defined if you want pg ref debugging */
#cmakedefine PG_DEBUG_REFS

/* Support ARMv8 CRC instructions */
#cmakedefine HAVE_ARMV8_CRC

/* Define if you have struct stat.st_mtimespec.tv_nsec */
#cmakedefine HAVE_STAT_ST_MTIMESPEC_TV_NSEC

/* Define if you have struct stat.st_mtim.tv_nsec */
#cmakedefine HAVE_STAT_ST_MTIM_TV_NSEC

/* Define if compiler supports static_cast<> */
#cmakedefine HAVE_STATIC_CAST

/* Version number of package */
#cmakedefine VERSION "@VERSION@"

#endif /* CONFIG_H */
