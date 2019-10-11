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

#include <cstdio>

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
  // FIPS zeroization audit 20191115: this memset is not security related.
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

int pipe_cloexec(int pipefd[2], int flags)
{
#if defined(HAVE_PIPE2)
  return pipe2(pipefd, O_CLOEXEC | flags);
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
#elif _WIN32
  /* TODO */
  return -ENOTSUP;
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

char *ceph_strerror_r(int errnum, char *buf, size_t buflen)
{
#ifdef _WIN32
  strerror_s(buf, buflen, errnum);
  return buf;
#elif defined(STRERROR_R_CHAR_P)
  return strerror_r(errnum, buf, buflen);
#else
  if (strerror_r(errnum, buf, buflen)) {
    snprintf(buf, buflen, "Unknown error %d", errnum);
  }
  return buf;
#endif
}

#ifdef _WIN32

#include <iomanip>
#include <ctime>

// chown is not available on Windows. Plus, changing file owners is not
// a common practice on Windows.
int chown(const char *path, uid_t owner, gid_t group) {
  return 0;
}

int fchown(int fd, uid_t owner, gid_t group) {
  return 0;
}

int lchown(const char *path, uid_t owner, gid_t group) {
  return 0;
}

int posix_memalign(void **memptr, size_t alignment, size_t size) {
  *memptr = _aligned_malloc(size, alignment);
  return *memptr ? 0 : errno;
}

char *strptime(const char *s, const char *format, struct tm *tm) {
  std::istringstream input(s);
  input.imbue(std::locale(setlocale(LC_ALL, nullptr)));
  input >> std::get_time(tm, format);
  if (input.fail()) {
    return nullptr;
  }
  return (char*)(s + input.tellg());
}

int pipe(int pipefd[2]) {
  // We'll use the same pipe size as Linux (64kb).
  return _pipe(pipefd, 0x10000, O_NOINHERIT);
}

// lrand48 is not available on Windows. We'll generate a pseudo-random
// value in the 0 - 2^31 range by calling rand twice.
long int lrand48(void) {
  long int val;
  val = (long int) rand();
  val << 16;
  val += (long int) rand();
  return val;
}

int fsync(int fd) {
  HANDLE handle = (HANDLE*)_get_osfhandle(fd);
  if (handle == INVALID_HANDLE_VALUE)
    return -1;
  if (!FlushFileBuffers(handle))
    return -1;
  return 0;
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
  DWORD bytes_written = 0;

  HANDLE handle = (HANDLE*)_get_osfhandle(fd);
  if (handle == INVALID_HANDLE_VALUE)
    return -1;

  OVERLAPPED overlapped = { 0 };
  ULARGE_INTEGER offsetUnion;
  offsetUnion.QuadPart = offset;

  overlapped.Offset = offsetUnion.LowPart;
  overlapped.OffsetHigh = offsetUnion.HighPart;

  if (!WriteFile(handle, buf, count, &bytes_written, &overlapped))
    // we may consider mapping error codes, although that may
    // not be exhaustive.
    return -1;

  return bytes_written;
}

ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
  DWORD bytes_read = 0;

  HANDLE handle = (HANDLE*)_get_osfhandle(fd);
  if (handle == INVALID_HANDLE_VALUE)
    return -1;

  OVERLAPPED overlapped = { 0 };
  ULARGE_INTEGER offsetUnion;
  offsetUnion.QuadPart = offset;

  overlapped.Offset = offsetUnion.LowPart;
  overlapped.OffsetHigh = offsetUnion.HighPart;

  if (!ReadFile(handle, buf, count, &bytes_read, &overlapped)) {
    if (GetLastError() != ERROR_HANDLE_EOF)
      return -1;
  }

  return bytes_read;
}

ssize_t preadv(int fd, const struct iovec *iov, int iov_cnt) {
  ssize_t read = 0;

  for (int i = 0; i < iov_cnt; i++) {
    int r = ::read(fd, iov[i].iov_base, iov[i].iov_len);
    if (r < 0)
      return r;
    read += r;
    if (r < iov[i].iov_len)
      break;
  }

  return read;
}

ssize_t writev(int fd, const struct iovec *iov, int iov_cnt) {
  ssize_t written = 0;

  for (int i = 0; i < iov_cnt; i++) {
    int r = ::write(fd, iov[i].iov_base, iov[i].iov_len);
    if (r < 0)
      return r;
    written += r;
    if (r < iov[i].iov_len)
      break;
  }

  return written;
}

#endif /* _WIN32 */
