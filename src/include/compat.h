/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 Stanislav Sedov <stas@FreeBSD.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_COMPAT_H
#define CEPH_COMPAT_H

#include "acconfig.h"
#include <sys/types.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

#if defined(__linux__)
#define PROCPREFIX
#endif

#include <fcntl.h>
#ifndef F_OFD_SETLK
#define F_OFD_SETLK F_SETLK 
#endif 

#include <sys/stat.h>
/*
 * FreeBSD has no statx(2)/struct statx (Linux-only, kernel >=4.11).
 * Emulate it over fstatat(2). FreeBSD's struct stat already carries
 * everything Linux needs statx() for, including st_birthtim -- which is
 * the one field plain stat() lacks on Linux and the whole reason statx()
 * exists there. So this is a straight field copy, not a real shim.
 */
#if defined(__FreeBSD__)
#ifndef CEPH_COMPAT_STATX_H
#define CEPH_COMPAT_STATX_H

#include <string.h>
#include <stdint.h>

struct statx_timestamp {
  int64_t  tv_sec;
  uint32_t tv_nsec;
  int32_t  __reserved;
};

struct statx {
  uint32_t stx_mask;
  uint32_t stx_blksize;
  uint64_t stx_attributes;
  uint32_t stx_nlink;
  uint32_t stx_uid;
  uint32_t stx_gid;
  uint16_t stx_mode;
  uint16_t __spare0[1];
  uint64_t stx_ino;
  uint64_t stx_size;
  uint64_t stx_blocks;
  uint64_t stx_attributes_mask;
  struct statx_timestamp stx_atime;
  struct statx_timestamp stx_btime;
  struct statx_timestamp stx_ctime;
  struct statx_timestamp stx_mtime;
  uint32_t stx_rdev_major;
  uint32_t stx_rdev_minor;
  uint32_t stx_dev_major;
  uint32_t stx_dev_minor;
  uint64_t stx_mnt_id;
  uint32_t stx_dio_mem_align;
  uint32_t stx_dio_offset_align;
  uint64_t __spare3[12];
};

#define STATX_TYPE        0x00000001U
#define STATX_MODE        0x00000002U
#define STATX_NLINK       0x00000004U
#define STATX_UID         0x00000008U
#define STATX_GID         0x00000010U
#define STATX_ATIME       0x00000020U
#define STATX_MTIME       0x00000040U
#define STATX_CTIME       0x00000080U
#define STATX_INO         0x00000100U
#define STATX_SIZE        0x00000200U
#define STATX_BLOCKS      0x00000400U
#define STATX_BASIC_STATS 0x000007ffU
#define STATX_BTIME       0x00000800U
#define STATX_ALL         0x00000fffU

/* Sync-behavior hints for network filesystems -- meaningless for a local
 * POSIX driver, defined only so call sites referencing them still compile. */
#ifndef AT_STATX_SYNC_AS_STAT
#define AT_STATX_SYNC_AS_STAT 0x0000
#endif
#ifndef AT_STATX_FORCE_SYNC
#define AT_STATX_FORCE_SYNC   0x2000
#endif
#ifndef AT_STATX_DONT_SYNC
#define AT_STATX_DONT_SYNC    0x4000
#endif

static inline int
statx(int dirfd, const char *pathname, int flags,
      unsigned int mask __unused, struct statx *stxbuf)
{
  struct stat st;
  int rc;

  /* AT_EMPTY_PATH + "" means "stat the fd itself" -- handle explicitly
   * rather than assume this FreeBSD's fstatat(2) supports the flag. */
  if (pathname && *pathname == '\0' && (flags & AT_EMPTY_PATH)) {
    rc = fstat(dirfd, &st);
  } else {
    rc = fstatat(dirfd, pathname, &st,
                 flags & (AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH));
  }
  if (rc < 0)
    return rc;

  memset(stxbuf, 0, sizeof(*stxbuf));
  stxbuf->stx_mask    = STATX_BASIC_STATS | STATX_BTIME;
  stxbuf->stx_blksize = st.st_blksize;
  stxbuf->stx_nlink   = st.st_nlink;
  stxbuf->stx_uid     = st.st_uid;
  stxbuf->stx_gid     = st.st_gid;
  stxbuf->stx_mode    = st.st_mode;
  stxbuf->stx_ino     = st.st_ino;
  stxbuf->stx_size    = st.st_size;
  stxbuf->stx_blocks  = st.st_blocks;

  stxbuf->stx_atime.tv_sec  = st.st_atim.tv_sec;
  stxbuf->stx_atime.tv_nsec = st.st_atim.tv_nsec;
  stxbuf->stx_mtime.tv_sec  = st.st_mtim.tv_sec;
  stxbuf->stx_mtime.tv_nsec = st.st_mtim.tv_nsec;
  stxbuf->stx_ctime.tv_sec  = st.st_ctim.tv_sec;
  stxbuf->stx_ctime.tv_nsec = st.st_ctim.tv_nsec;
  stxbuf->stx_btime.tv_sec  = st.st_birthtim.tv_sec;
  stxbuf->stx_btime.tv_nsec = st.st_birthtim.tv_nsec;

  stxbuf->stx_rdev_major = major(st.st_rdev);
  stxbuf->stx_rdev_minor = minor(st.st_rdev);
  stxbuf->stx_dev_major  = major(st.st_dev);
  stxbuf->stx_dev_minor  = minor(st.st_dev);

  return 0;
}

#endif // CEPH_COMPAT_STATX_H
#endif // __FreeBSD__

#ifdef _WIN32
#include "include/win32/fs_compat.h"
#endif

#ifndef ACCESSPERMS
#define ACCESSPERMS (S_IRWXU|S_IRWXG|S_IRWXO)
#endif

#ifndef ALLPERMS
#define ALLPERMS (S_ISUID|S_ISGID|S_ISVTX|S_IRWXU|S_IRWXG|S_IRWXO)
#endif

#if defined(__FreeBSD__)

// FreeBSD supports Linux procfs with its compatibility module
// And all compatibility stuff is standard mounted on this 
#define PROCPREFIX "/compat/linux"

#ifndef MSG_MORE
#define MSG_MORE 0
#endif

#ifndef O_DSYNC
#define O_DSYNC O_SYNC
#endif
#ifndef O_RSYNC
#define O_RSYNC O_SYNC
#endif

/* And include the extra required include file */
#include <pthread_np.h>

#include <sys/param.h>
#include <sys/cpuset.h>

#endif /* __FreeBSD__ */

#if defined(__APPLE__)
struct cpu_set_t;
#endif

#if defined(__APPLE__) || defined(__FreeBSD__)
/* Make sure that ENODATA is defined in the correct way */
#ifdef ENODATA
#if (ENODATA == 9919)
// #warning ENODATA already defined to be 9919, redefining to fix
// Silencing this warning because it fires at all files where compat.h
// is included after boost files.
//
// This value stems from the definition in the boost library
// And when this case occurs it is due to the fact that boost files
// are included before this file. Redefinition might not help in this
// case since already parsed code has evaluated to the wrong value.
// This would warrrant for d definition that would actually be evaluated
// at the location of usage and report a possible conflict.
// This is left up to a future improvement
#elif (ENODATA != 87)
// #warning ENODATA already defined to a value different from 87 (ENOATRR), refining to fix
#endif
#undef ENODATA
#endif
#define ENODATA ENOATTR

// Fix clock accuracy
#if !defined(CLOCK_MONOTONIC_COARSE)
#if defined(CLOCK_MONOTONIC_FAST)
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC_FAST
#else
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif
#endif
#if !defined(CLOCK_REALTIME_COARSE)
#if defined(CLOCK_REALTIME_FAST)
#define CLOCK_REALTIME_COARSE CLOCK_REALTIME_FAST
#else
#define CLOCK_REALTIME_COARSE CLOCK_REALTIME
#endif
#endif

/* get PATH_MAX */
#include <limits.h>

#ifndef EUCLEAN
#define EUCLEAN 117
#endif
#ifndef EREMOTEIO
#define EREMOTEIO 121
#endif
#ifndef EKEYREJECTED
#define EKEYREJECTED 129
#endif

#ifndef XATTR_CREATE
#define XATTR_CREATE  1
#endif
#ifndef XATTR_REPLACE
#define XATTR_REPLACE 2
#endif

#endif /* __APPLE__ || __FreeBSD__ */

#ifndef HOST_NAME_MAX
#ifdef MAXHOSTNAMELEN 
#define HOST_NAME_MAX MAXHOSTNAMELEN 
#else
#define HOST_NAME_MAX 255
#endif
#endif /* HOST_NAME_MAX */

/* O_LARGEFILE is not defined/required on OSX/FreeBSD */
#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif

/* Could be relevant for other platforms */
#ifndef ERESTART
#define ERESTART EINTR
#endif

#ifndef TEMP_FAILURE_RETRY
#define TEMP_FAILURE_RETRY(expression) ({     \
  __typeof(expression) __result;              \
  do {                                        \
    __result = (expression);                  \
  } while (__result == -1 && errno == EINTR); \
  __result; })
#endif

#ifdef __cplusplus
# define VOID_TEMP_FAILURE_RETRY(expression) \
   static_cast<void>(TEMP_FAILURE_RETRY(expression))
#else
# define VOID_TEMP_FAILURE_RETRY(expression) \
   do { (void)TEMP_FAILURE_RETRY(expression); } while (0)
#endif

#if defined(__FreeBSD__) || defined(__APPLE__)
#define lseek64(fd, offset, whence) lseek(fd, offset, whence)
#endif

#if defined(__sun) || defined(_AIX)
#define LOG_AUTHPRIV    (10<<3)
#define LOG_FTP         (11<<3)
#define __STRING(x)     "x"
#endif

#if defined(__sun) || defined(_AIX) || defined(_WIN32)
#define IFTODT(mode)   (((mode) & 0170000) >> 12)
#endif

#if defined(_AIX)
#define MSG_DONTWAIT MSG_NONBLOCK
#endif

#define pthread_kill_unsupported_helper(thread, signal) ({ \
  int __i = -ENOTSUP;                                      \
  __i; })

#if defined(_WIN32) && defined(__clang__) && \
    !defined(_LIBCPP_HAS_THREAD_API_PTHREAD)
  #define ceph_pthread_kill pthread_kill_unsupported_helper
#else
  #define ceph_pthread_kill pthread_kill
#endif

int ceph_posix_fallocate(int fd, off_t offset, off_t len);

#ifdef __cplusplus
extern "C" {
#endif

int ceph_pthread_getname(char* name, size_t size);
int ceph_pthread_setname(const char* name);

int pipe_cloexec(int pipefd[2], int flags);
char *ceph_strerror_r(int errnum, char *buf, size_t buflen);
unsigned get_page_size();
// On success, returns the number of bytes written to the buffer. On
// failure, returns -1.
ssize_t get_self_exe_path(char* path, int buff_length);

int ceph_memzero_s(void *dest, size_t destsz, size_t count);

#ifdef __cplusplus
}
#endif

#if defined(_WIN32)

#include "include/win32/winsock_compat.h"

#include <windows.h>
#include <time.h>

#include "include/win32/win32_errno.h"

// There are a few name collisions between Windows headers and Ceph.
// Updating Ceph definitions would be the prefferable fix in order to avoid
// confussion, unless it requires too many changes, in which case we're going
// to redefine Windows values by adding the "WIN32_" prefix.
#define WIN32_DELETE 0x00010000L
#undef DELETE

#define WIN32_ERROR 0
#undef ERROR

#ifndef uint
typedef unsigned int uint;
#endif

typedef _sigset_t sigset_t;

typedef unsigned int blksize_t;
typedef unsigned __int64 blkcnt_t;
typedef unsigned short nlink_t;

typedef long long loff_t;

#define CPU_SETSIZE (sizeof(size_t)*8)

typedef union
{
  char cpuset[CPU_SETSIZE/8];
  size_t _align;
} cpu_set_t;

struct iovec {
  void *iov_base;
  size_t iov_len;
};

#define SHUT_RD SD_RECEIVE
#define SHUT_WR SD_SEND
#define SHUT_RDWR SD_BOTH

#ifndef SIGINT
#define SIGINT 2
#endif

#ifndef SIGKILL
#define SIGKILL 9
#endif

#define IOV_MAX 1024

#ifdef __cplusplus
extern "C" {
#endif

ssize_t readv(int fd, const struct iovec *iov, int iov_cnt);
ssize_t writev(int fd, const struct iovec *iov, int iov_cnt);

int fsync(int fd);
ssize_t pread(int fd, void *buf, size_t count, off_t offset);
ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);

long int lrand48(void);
int random();

int pipe(int pipefd[2]);

int posix_memalign(void **memptr, size_t alignment, size_t size);

char *strptime(const char *s, const char *format, struct tm *tm);

int chown(const char *path, uid_t owner, gid_t group);
int fchown(int fd, uid_t owner, gid_t group);
int lchown(const char *path, uid_t owner, gid_t group);
int setenv(const char *name, const char *value, int overwrite);

int geteuid();
int getegid();
int getuid();
int getgid();

#define unsetenv(name) _putenv_s(name, "")

int win_socketpair(int socks[2]);

#ifdef __MINGW32__
extern _CRTIMP errno_t __cdecl _putenv_s(const char *_Name,const char *_Value);

#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#define htobe16(x) __builtin_bswap16(x)
#define htole16(x) (x)
#define be16toh(x) __builtin_bswap16(x)
#define le16toh(x) (x)

#define htobe32(x) __builtin_bswap32(x)
#define htole32(x) (x)
#define be32toh(x) __builtin_bswap32(x)
#define le32toh(x) (x)

#define htobe64(x) __builtin_bswap64(x)
#define htole64(x) (x)
#define be64toh(x) __builtin_bswap64(x)
#define le64toh(x) (x)
#endif // defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)

#endif // __MINGW32__

#ifdef __cplusplus
}
#endif

#define compat_closesocket closesocket
// Use "aligned_free" when freeing memory allocated using posix_memalign or
// _aligned_malloc. Using "free" will crash.
static inline void aligned_free(void* ptr) {
  _aligned_free(ptr);
}

// O_CLOEXEC is not defined on Windows. Since handles aren't inherited
// with subprocesses unless explicitly requested, we'll define this
// flag as a no-op.
#define O_CLOEXEC 0
#define SOCKOPT_VAL_TYPE char*

#define DEV_NULL "nul"

#else /* WIN32 */

#define SOCKOPT_VAL_TYPE void*

static inline void aligned_free(void* ptr) {
  free(ptr);
}
static inline int compat_closesocket(int fildes) {
  return close(fildes);
}

#define DEV_NULL "/dev/null"

#endif /* WIN32 */

/* Supplies code to be run at startup time before invoking main().
 * Use as:
 *
 *     CEPH_CONSTRUCTOR(my_constructor) {
 *         ...some code...
 *     }
 */
#ifdef _MSC_VER
#pragma section(".CRT$XCU",read)
#define CEPH_CONSTRUCTOR(f) \
  static void __cdecl f(void); \
  __declspec(allocate(".CRT$XCU")) static void (__cdecl*f##_)(void) = f; \
  static void __cdecl f(void)
#else
#define CEPH_CONSTRUCTOR(f) \
  static void f(void) __attribute__((constructor)); \
  static void f(void)
#endif

/* This should only be used with the socket API. */
static inline int ceph_sock_errno() {
#ifdef _WIN32
  return wsae_to_errno(WSAGetLastError());
#else
  return errno;
#endif
}

// Needed on Windows when handling binary files. Without it, line
// endings will be replaced and certain characters can be treated as
// EOF.
#ifndef O_BINARY
#define O_BINARY 0
#endif

#endif /* !CEPH_COMPAT_H */
