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

#if defined(__linux__)
#define PROCPREFIX
#endif

#include <sys/stat.h>
#ifndef ACCESSPERMS
#define ACCESSPERMS (S_IRWXU|S_IRWXG|S_IRWXO)
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

/* And include the extra required include file */
#include <pthread_np.h>

#include <sys/param.h>
#include <sys/cpuset.h>
#define cpu_set_t cpuset_t
int sched_setaffinity(pid_t pid, size_t cpusetsize,
                      cpu_set_t *mask);

#endif /* __FreeBSD__ */

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
#define XATTR_CREATE 1
#endif

#endif /* __APPLE__ */

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
#define IFTODT(mode)   (((mode) & 0170000) >> 12)
#endif

#if defined(_AIX)
#define MSG_DONTWAIT MSG_NONBLOCK
#endif

#if defined(HAVE_PTHREAD_SETNAME_NP)
  #if defined(__APPLE__)
    #define ceph_pthread_setname(thread, name) ({ \
      int __result = 0;                         \
      if (thread == pthread_self())             \
        __result = pthread_setname_np(name);    \
      __result; })
  #else
    #define ceph_pthread_setname pthread_setname_np
  #endif
#elif defined(HAVE_PTHREAD_SET_NAME_NP)
  /* Fix a small name diff and return 0 */
  #define ceph_pthread_setname(thread, name) ({ \
    pthread_set_name_np(thread, name);          \
    0; })
#else
  /* compiler warning free success noop */
  #define ceph_pthread_setname(thread, name) ({ \
    int __i = 0;                              \
    __i; })
#endif

#if defined(HAVE_PTHREAD_GETNAME_NP)
  #define ceph_pthread_getname pthread_getname_np
#elif defined(HAVE_PTHREAD_GET_NAME_NP)
  #define ceph_pthread_getname(thread, name, len) ({ \
    pthread_get_name_np(thread, name, len);          \
    0; })
#else
  /* compiler warning free success noop */
  #define ceph_pthread_getname(thread, name, len) ({ \
    if (name != NULL)                              \
      *name = '\0';                                \
    0; })
#endif

int ceph_posix_fallocate(int fd, off_t offset, off_t len);

#ifdef __cplusplus
extern "C" {
#endif

int pipe_cloexec(int pipefd[2], int flags);
char *ceph_strerror_r(int errnum, char *buf, size_t buflen);

#ifdef __cplusplus
}
#endif

#if defined(_WIN32)

typedef _sigset_t sigset_t;

typedef int uid_t;
typedef int gid_t;

typedef long blksize_t;
typedef long blkcnt_t;
typedef long nlink_t;

typedef long long loff_t;

#define SHUT_RD SD_RECEIVE
#define SHUT_WR SD_SEND
#define SHUT_RDWR SD_BOTH

#ifndef SIGINT
#define SIGINT 2
#endif

#ifndef SIGKILL
#define SIGKILL 9
#endif

#ifndef ENODATA
// mingw doesn't define this, the Windows SDK does.
#define ENODATA 120
#endif

#define ESHUTDOWN ECONNABORTED
#define ESTALE 256
#define EREMOTEIO 257

// O_CLOEXEC is not defined on Windows. Since handles aren't inherited
// with subprocesses unless explicitly requested, we'll define this
// flag as a no-op.
#define O_CLOEXEC 0

#endif /* WIN32 */

#endif /* !CEPH_COMPAT_H */
