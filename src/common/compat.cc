// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#if defined(__linux__) 
#include <sys/vfs.h>
#endif

#include "include/compat.h"
#include "include/sock_compat.h"
#include "common/safe_io.h"

// The type-value for a ZFS FS in fstatfs.
#define FS_ZFS_TYPE 0xde

// On FreeBSD, ZFS fallocate always fails since it is considered impossible to
// reserve space on a COW filesystem. posix_fallocate() returns EINVAL
// Linux in this case already emulates the reservation in glibc
// In which case it is allocated manually, and still that is not a real guarantee
// that a full buffer is allocated on disk, since it could be compressed.
// To prevent this the written buffer needs to be loaded with random data.
int manual_fallocate(int fd, off_t offset, off_t len) {
  int r = lseek(fd, offset, SEEK_SET);
  if (r == -1)
    return errno;
  char data[1024*128];
  // TODO: compressing filesystems would require random data
  memset(data, 0x42, sizeof(data));
  for (off_t off = 0; off < len; off += sizeof(data)) {
    if (off + static_cast<off_t>(sizeof(data)) > len)
      r = safe_write(fd, data, len - off);
    else
      r = safe_write(fd, data, sizeof(data));
    if (r == -1) {
      return errno;
    }
  }
  return 0;
}

int on_zfs(int basedir_fd) {
  struct statfs basefs;
  (void)fstatfs(basedir_fd, &basefs);
  return (basefs.f_type == FS_ZFS_TYPE);
}

int ceph_posix_fallocate(int fd, off_t offset, off_t len) {
  // Return 0 if oke, otherwise errno > 0

#ifdef HAVE_POSIX_FALLOCATE
  if (on_zfs(fd)) {
    return manual_fallocate(fd, offset, len);
  } else {
    return posix_fallocate(fd, offset, len);
  }
#elif defined(__APPLE__)
  fstore_t store;
  store.fst_flags = F_ALLOCATECONTIG;
  store.fst_posmode = F_PEOFPOSMODE;
  store.fst_offset = offset;
  store.fst_length = len;

  int ret = fcntl(fd, F_PREALLOCATE, &store);
  if (ret == -1) {
    ret = errno;
  }
  return ret;
#else
  return manual_fallocate(fd, offset, len);
#endif
} 

int pipe_cloexec(int pipefd[2])
{
#if defined(HAVE_PIPE2)
  return pipe2(pipefd, O_CLOEXEC);
#else
  if (pipe(pipefd) == -1)
    return -1;

  /*
   * The old-fashioned, race-condition prone way that we have to fall
   * back on if pipe2 does not exist.
   */
  if (fcntl(pipefd[0], F_SETFD, FD_CLOEXEC) < 0) {
    goto fail;
  }

  if (fcntl(pipefd[1], F_SETFD, FD_CLOEXEC) < 0) {
    goto fail;
  }

  return 0;
fail:
  int save_errno = errno;
  VOID_TEMP_FAILURE_RETRY(close(pipefd[0]));
  VOID_TEMP_FAILURE_RETRY(close(pipefd[1]));
  return (errno = save_errno, -1);
#endif
}


int socket_cloexec(int domain, int type, int protocol)
{
#ifdef SOCK_CLOEXEC
  return socket(domain, type|SOCK_CLOEXEC, protocol);
#else
  int fd = socket(domain, type, protocol);
  if (fd == -1)
    return -1;

  if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
    goto fail;

  return fd;
fail:
  int save_errno = errno;
  VOID_TEMP_FAILURE_RETRY(close(fd));
  return (errno = save_errno, -1);
#endif
}

int socketpair_cloexec(int domain, int type, int protocol, int sv[2])
{
#ifdef SOCK_CLOEXEC
  return socketpair(domain, type|SOCK_CLOEXEC, protocol, sv);
#else
  int rc = socketpair(domain, type, protocol, sv);
  if (rc == -1)
    return -1;

  if (fcntl(sv[0], F_SETFD, FD_CLOEXEC) < 0)
    goto fail;

  if (fcntl(sv[1], F_SETFD, FD_CLOEXEC) < 0)
    goto fail;

  return 0;
fail:
  int save_errno = errno;
  VOID_TEMP_FAILURE_RETRY(close(sv[0]));
  VOID_TEMP_FAILURE_RETRY(close(sv[1]));
  return (errno = save_errno, -1);
#endif
}

int accept_cloexec(int sockfd, struct sockaddr* addr, socklen_t* addrlen)
{
#ifdef HAVE_ACCEPT4
  return accept4(sockfd, addr, addrlen, SOCK_CLOEXEC);
#else
  int fd = accept(sockfd, addr, addrlen);
  if (fd == -1)
    return -1;

  if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
    goto fail;

  return fd;
fail:
  int save_errno = errno;
  VOID_TEMP_FAILURE_RETRY(close(fd));
  return (errno = save_errno, -1);
#endif
}

#if defined(__FreeBSD__)
int sched_setaffinity(pid_t pid, size_t cpusetsize,
                      cpu_set_t *mask)
{
  return 0;
}
#endif

