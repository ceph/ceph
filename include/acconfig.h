/* config.h file expanded by Cmake for build */

#ifndef CONFIG_H
#define CONFIG_H

/* Define to 1 if you have the `memset_s()` function. */
/* #undef HAVE_MEMSET_S */

/* fallocate(2) is supported */
#define CEPH_HAVE_FALLOCATE

/* Define to 1 if you have the `posix_fadvise' function. */
#define HAVE_POSIX_FADVISE 1

/* Define to 1 if you have the `posix_fallocate' function. */
#define HAVE_POSIX_FALLOCATE 1

/* Define to 1 if you have the `syncfs' function. */
#define HAVE_SYS_SYNCFS 1

/* sync_file_range(2) is supported */
#define HAVE_SYNC_FILE_RANGE

/* Define if you have mallinfo */
/* #undef HAVE_MALLINFO */

/* Define to 1 if you have the `pwritev' function. */
#define HAVE_PWRITEV 1

/* Define to 1 if you have the <sys/mount.h> header file. */
#define HAVE_SYS_MOUNT_H 1

/* Define to 1 if you have the <sys/param.h> header file. */
#define HAVE_SYS_PARAM_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/vfs.h> header file. */
#define HAVE_SYS_VFS_H 1

/* Define to 1 if you have the <execinfo.h> header file. */
#define HAVE_EXECINFO_H 1

/* Define to 1 if the system has the type `__s16'. */
#define HAVE___S16 1

/* Define to 1 if the system has the type `__s32'. */
#define HAVE___S32 1

/* Define to 1 if the system has the type `__s64'. */
#define HAVE___S64 1

/* Define to 1 if the system has the type `__s8'. */
#define HAVE___S8 1

/* Define to 1 if the system has the type `__u16'. */
#define HAVE___U16 1

/* Define to 1 if the system has the type `__u32'. */
#define HAVE___U32 1

/* Define to 1 if the system has the type `__u64'. */
#define HAVE___U64 1

/* Define to 1 if the system has the type `__u8'. */
#define HAVE___U8 1

/* Define if the system has the type `in_addr_t' */
#define HAVE_IN_ADDR_T

/* Define if you have suseconds_t */
#define HAVE_SUSECONDS_T

/* Define if you have res_nquery */
#define HAVE_RES_NQUERY

/* Defined if you have LZ4 */
#define HAVE_LZ4

/* Defined if you have BROTLI */
/* #undef HAVE_BROTLI */

/* Defined if you have libaio */
#define HAVE_LIBAIO

/* Defined if you have libzbd */
/* #undef HAVE_LIBZBD */

/* Defined if you have liburing */
#define HAVE_LIBURING

/* Defind if you have POSIX AIO */
/* #undef HAVE_POSIXAIO */

/* Defined if OpenLDAP enabled */
#define HAVE_OPENLDAP

/* Define if you have fuse */
#define HAVE_LIBFUSE

/* Define version major */
#define CEPH_FUSE_MAJOR_VERSION 2

/* Define version minor */
#define CEPH_FUSE_MINOR_VERSION 9

/* Define to 1 if you have libxfs */
#define HAVE_LIBXFS 1

/* SPDK conditional compilation */
/* #undef HAVE_SPDK */

/* DPDK conditional compilation */
/* #undef HAVE_DPDK */

/* PMEM_DEVICE (OSD) conditional compilation */
/* #undef HAVE_BLUESTORE_PMEM */

/* Define if you have tcmalloc */
#define HAVE_LIBTCMALLOC
/* #undef LIBTCMALLOC_MISSING_ALIGNED_ALLOC */

/* AsyncMessenger RDMA conditional compilation */
#define HAVE_RDMA

/* ibverbs experimental conditional compilation */
/* #undef HAVE_IBV_EXP */

/* define if bluestore enabled */
#define WITH_BLUESTORE

/* define if cephfs enabled */
/* #undef WITH_CEPHFS */

/* define if systemed is enabled */
#define WITH_SYSTEMD

/*define if GSSAPI/KRB5 enabled */
/* #undef HAVE_GSSAPI */

/* define if rbd enabled */
/* #undef WITH_RBD */

/* define if kernel rbd enabled */
/* #undef WITH_KRBD */

/* define if key-value-store is enabled */
/* #undef WITH_KVS */

/* define if radosgw enabled */
#define WITH_RADOSGW

/* define if radosgw has openssl support */
/* #undef WITH_CURL_OPENSSL */

/* define if HAVE_THREAD_SAFE_RES_QUERY */
/* #undef HAVE_THREAD_SAFE_RES_QUERY */

/* define if HAVE_REENTRANT_STRSIGNAL */
/* #undef HAVE_REENTRANT_STRSIGNAL */

/* Define if you want to use LTTng */
/* #undef WITH_LTTNG */

/* Define if you want to use Jaeger */
/* #undef HAVE_JAEGER */

/* Define if you want to use EVENTTRACE */
/* #undef WITH_EVENTTRACE */

/* Define if you want to OSD function instrumentation */
/* #undef WITH_OSD_INSTRUMENT_FUNCTIONS */

/* Define if you want to use Babeltrace */
/* #undef WITH_BABELTRACE */

/* Define to 1 if you have the <babeltrace/babeltrace.h> header file. */
/* #undef HAVE_BABELTRACE_BABELTRACE_H */

/* Define to 1 if you have the <babeltrace/ctf/events.h> header file. */
/* #undef HAVE_BABELTRACE_CTF_EVENTS_H */

/* Define to 1 if you have the <babeltrace/ctf/iterator.h> header file. */
/* #undef HAVE_BABELTRACE_CTF_ITERATOR_H */

/* Define to 1 if you have the <arpa/nameser_compat.h> header file. */
#define HAVE_ARPA_NAMESER_COMPAT_H 1

/* FastCGI headers are in /usr/include/fastcgi */
/* #undef FASTCGI_INCLUDE_DIR */

/* splice(2) is supported */
#define CEPH_HAVE_SPLICE

/* Define if you want C_Gather debugging */
#define DEBUG_GATHER

/* Define to 1 if you have the `getgrouplist' function. */
#define HAVE_GETGROUPLIST 1

/* LTTng is disabled, so define this macro to be nothing. */
/* #undef tracepoint */

/* Define to 1 if you have fdatasync. */
#define HAVE_FDATASYNC 1

/* Define to 1 if you have the <valgrind/helgrind.h> header file. */
#define HAVE_VALGRIND_HELGRIND_H 1

/* Define to 1 if you have the <sys/prctl.h> header file. */
#define HAVE_SYS_PRCTL_H 1

/* Define to 1 if you have the <linux/types.h> header file. */
#define HAVE_LINUX_TYPES_H 1

/* Define to 1 if you have the <linux/version.h> header file. */
#define HAVE_LINUX_VERSION_H 1

/* Define to 1 if you have sched.h. */
#define HAVE_SCHED 1

/* Define to 1 if you have sigdescr_np. */
/* #undef HAVE_SIGDESCR_NP */

/* Support SSE (Streaming SIMD Extensions) instructions */
/* #undef HAVE_SSE */

/* Support SSE2 (Streaming SIMD Extensions 2) instructions */
/* #undef HAVE_SSE2 */

/* Define to 1 if you have the `pipe2' function. */
#define HAVE_PIPE2 1

/* Support NEON instructions */
/* #undef HAVE_NEON */

/* Define if you have pthread_spin_init */
#define HAVE_PTHREAD_SPINLOCK

/* name_to_handle_at exists */
#define HAVE_NAME_TO_HANDLE_AT

/* we have a recent nasm and are x86_64 */
#define HAVE_NASM_X64

/* nasm can also build the isa-l:avx512 */
#define HAVE_NASM_X64_AVX512

/* Define if the erasure code isa-l plugin is compiled */
#define WITH_EC_ISA_PLUGIN

/* Define to 1 if strerror_r returns char *. */
#define STRERROR_R_CHAR_P 1

/* Defined if you have libzfs enabled */
/* #undef HAVE_LIBZFS */

/* Define if the C compiler supports __func__ */
#define HAVE_FUNC

/* Define if the C compiler supports __PRETTY_FUNCTION__ */
#define HAVE_PRETTY_FUNC

/* Define if the C compiler supports __attribute__((__symver__ (".."))) */
/* #undef HAVE_ATTR_SYMVER */

/* Define if the C compiler supports __asm__(".symver ..") */
#define HAVE_ASM_SYMVER

/* Have eventfd extension. */
#define HAVE_EVENTFD

/* Define if enabling coverage. */
/* #undef ENABLE_COVERAGE */

/* Defined if you want pg ref debugging */
/* #undef PG_DEBUG_REFS */

/* Support ARMv8 CRC instructions */
/* #undef HAVE_ARMV8_CRC */

/* Support ARMv8 CRYPTO instructions */
/* #undef HAVE_ARMV8_CRYPTO */

/* Support ARMv8 CRC and CRYPTO intrinsics */
/* #undef HAVE_ARMV8_CRC_CRYPTO_INTRINSICS */

/* Define if you have struct stat.st_mtimespec.tv_nsec */
/* #undef HAVE_STAT_ST_MTIMESPEC_TV_NSEC */

/* Define if you have struct stat.st_mtim.tv_nsec */
#define HAVE_STAT_ST_MTIM_TV_NSEC

/* Define if compiler supports static_cast<> */
/* #undef HAVE_STATIC_CAST */

/* Version number of package */
#define PROJECT_VERSION "17.0.0"

/* Defined if pthread_setname_np() is available */
#define HAVE_PTHREAD_SETNAME_NP 1

/* Defined if pthread_rwlockattr_setkind_np() is available */
#define HAVE_PTHREAD_RWLOCKATTR_SETKIND_NP

/* Defined if blkin enabled */
/* #undef WITH_BLKIN */

/* Defined if pthread_set_name_np() is available */
/* #undef HAVE_PTHREAD_SET_NAME_NP */

/* Defined if pthread_getname_np() is available */
#define HAVE_PTHREAD_GETNAME_NP 1

/* Support POWER8 instructions */
/* #undef HAVE_POWER8 */

/* Define if endian type is big endian */
/* #undef CEPH_BIG_ENDIAN */

/* Define if endian type is little endian */
#define CEPH_LITTLE_ENDIAN

#define MGR_PYTHON_EXECUTABLE "/usr/bin/python3.8"

/* Define to 1 if you have the `getprogname' function. */
/* #undef HAVE_GETPROGNAME */

/* Defined if getentropy() is available */
#define HAVE_GETENTROPY

/* Defined if libradosstriper is enabled: */
#define WITH_LIBRADOSSTRIPER

/* Defined if OpenSSL is available for the rgw beast frontend */
#define WITH_RADOSGW_BEAST_OPENSSL

/* Defined if rabbitmq-c is available for rgw amqp push endpoint */
#define WITH_RADOSGW_AMQP_ENDPOINT

/* Defined if libedkafka is available for rgw kafka push endpoint */
#define WITH_RADOSGW_KAFKA_ENDPOINT

/* Defined if lua packages can be installed by radosgw */
#define WITH_RADOSGW_LUA_PACKAGES

/* Backend dbstore for Rados Gateway */
#define WITH_RADOSGW_DBSTORE

/* Backend CORTX-Motr for Rados Gateway */
/* #undef WITH_RADOSGW_MOTR */

/* Defined if std::map::merge() is supported */
#define HAVE_STDLIB_MAP_SPLICING

/* Defined if Intel QAT compress/decompress is supported */
/* #undef HAVE_QATZIP */

/* Define if seastar is available. */
/* #undef HAVE_SEASTAR */

/* Define if unit tests are built. */
#define UNIT_TESTS_BUILT

/* Define if RBD QCOW migration format is enabled */
/* #undef WITH_RBD_MIGRATION_FORMAT_QCOW_V1 */

/* Define if libcephsqlite is enabled */
#define WITH_LIBCEPHSQLITE

/* Define if RWL is enabled */
/* #undef WITH_RBD_RWL */

/* Define if PWL-SSD is enabled */
/* #undef WITH_RBD_SSD_CACHE */

/* Define if libcryptsetup can be used (linux only) */
/* #undef HAVE_LIBCRYPTSETUP */

/* Shared library extension, such as .so, .dll or .dylib */
#define CMAKE_SHARED_LIBRARY_SUFFIX ".so"

/* libexec directory path */
#define CMAKE_INSTALL_LIBEXECDIR "libexec"

#endif /* CONFIG_H */
