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

#include "include/compat.h"
#include "include/sock_compat.h"
#include "common/safe_io.h"

#include <cstdio>
#include <sstream>

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include "acconfig.h"
#ifdef HAVE_MEMSET_S
# define __STDC_WANT_LIB_EXT1__ 1
#endif
#include <string.h>
#include <thread>
#ifndef _WIN32
#include <sys/mount.h>
#else
#include <stdlib.h>
#endif
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#if defined(__linux__) 
#include <sys/vfs.h>
#endif

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
  #ifndef _WIN32
  struct statfs basefs;
  (void)fstatfs(basedir_fd, &basefs);
  return (basefs.f_type == FS_ZFS_TYPE);
  #else
  return 0;
  #endif
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

  #ifndef _WIN32
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
  #endif

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

  #ifndef _WIN32
  if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
    goto fail;
  #endif

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

  #ifndef _WIN32
  if (fcntl(sv[0], F_SETFD, FD_CLOEXEC) < 0)
    goto fail;

  if (fcntl(sv[1], F_SETFD, FD_CLOEXEC) < 0)
    goto fail;
  #endif

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

  #ifndef _WIN32
  if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
    goto fail;
  #endif

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

int ceph_memzero_s(void *dest, size_t destsz, size_t count) {
#ifdef HAVE_MEMSET_S
    return memset_s(dest, destsz, 0, count);
#elif defined(_WIN32)
    SecureZeroMemory(dest, count);
#else
    explicit_bzero(dest, count);
#endif
    return 0;
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
  val <<= 16;
  val += (long int) rand();
  return val;
}

int random() {
  return rand();
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

int &alloc_tls() {
  static __thread int tlsvar;
  tlsvar++;
  return tlsvar;
}

void apply_tls_workaround() {
  // Workaround for the following Mingw bugs:
  // https://sourceforge.net/p/mingw-w64/bugs/727/
  // https://sourceforge.net/p/mingw-w64/bugs/527/
  // https://sourceforge.net/p/mingw-w64/bugs/445/
  // https://gcc.gnu.org/bugzilla/attachment.cgi?id=41382
  pthread_key_t key;
  pthread_key_create(&key, nullptr);
  // Use a TLS slot for emutls
  alloc_tls();
  // Free up a slot that can now be used for c++ destructors
  pthread_key_delete(key);
}

CEPH_CONSTRUCTOR(ceph_windows_init) {
  // This will run at startup time before invoking main().
  WSADATA wsaData;
  int error;

  #ifdef __MINGW32__
  apply_tls_workaround();
  #endif

  error = WSAStartup(MAKEWORD(2, 2), &wsaData);
  if (error != 0) {
    fprintf(stderr, "WSAStartup failed: %d", WSAGetLastError());
    exit(error);
  }
}

int _win_socketpair(int socks[2])
{
  union {
     struct sockaddr_in inaddr;
     struct sockaddr addr;
  } a;
  SOCKET listener;
  int e;
  socklen_t addrlen = sizeof(a.inaddr);
  int reuse = 1;

  if (socks == 0) {
    WSASetLastError(WSAEINVAL);
    return -1;
  }

  listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (listener == INVALID_SOCKET) {
    return -1;
  }

  memset(&a, 0, sizeof(a));
  a.inaddr.sin_family = AF_INET;
  a.inaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  a.inaddr.sin_port = 0;

  socks[0] = socks[1] = -1;
  SOCKET s[2] = { INVALID_SOCKET, INVALID_SOCKET };

  do {
    if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR,
           (char*) &reuse, (socklen_t) sizeof(reuse)) == -1)
      break;
    if (bind(listener, &a.addr, sizeof(a.inaddr)) == SOCKET_ERROR)
      break;
    if (getsockname(listener, &a.addr, &addrlen) == SOCKET_ERROR)
      break;
    if (listen(listener, 1) == SOCKET_ERROR)
      break;
    s[0] = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s[0] == INVALID_SOCKET)
      break;
    if (connect(s[0], &a.addr, sizeof(a.inaddr)) == SOCKET_ERROR)
      break;
    s[1] = accept(listener, NULL, NULL);
    if (s[1] == INVALID_SOCKET)
      break;

    closesocket(listener);

    // The Windows socket API is mostly compatible with the Berkeley
    // API, with a few exceptions. The Windows socket functions use
    // SOCKET instead of int. The issue is that on x64 systems,
    // SOCKET uses 64b while int uses 32b. There's been much debate
    // whether casting a Windows socket to an int is safe or not.
    // Worth noting that Windows kernel objects use 32b. For now,
    // we're just adding a check.
    //
    // Ideally, we should update ceph to use the right type but this
    // can be quite difficult, especially considering that there are
    // a significant number of functions that accept both sockets and
    // file descriptors.
    if (s[0] >> 32 || s[1] >> 32) {
      WSASetLastError(WSAENAMETOOLONG);
      break;
    }

    socks[0] = s[0];
    socks[1] = s[1];

    return 0;

  } while (0);

  e = WSAGetLastError();
  closesocket(listener);
  closesocket(s[0]);
  closesocket(s[1]);
  WSASetLastError(e);
  return -1;
}

int win_socketpair(int socks[2]) {
  int r = 0;
  for (int i = 0; i < 15; i++) {
    r = _win_socketpair(socks);
    if (r && WSAGetLastError() == WSAEADDRINUSE) {
      sleep(2);
      continue;
    }
    else {
      break;
    }
  }
  return r;
}

unsigned get_page_size() {
  SYSTEM_INFO system_info;
  GetSystemInfo(&system_info);
  return system_info.dwPageSize;
}

int setenv(const char *name, const char *value, int overwrite) {
  if (!overwrite && getenv(name)) {
    return 0;
  }
  return _putenv_s(name, value);
}

ssize_t get_self_exe_path(char* path, int buff_length) {
  return GetModuleFileName(NULL, path, buff_length - 1);
}

int geteuid()
{
  return 0;
}

int getegid()
{
  return 0;
}

int getuid()
{
  return 0;
}

int getgid()
{
  return 0;
}

#else

unsigned get_page_size() {
  return sysconf(_SC_PAGESIZE);
}

ssize_t get_self_exe_path(char* path, int buff_length) {
  return readlink("/proc/self/exe", path,
                  sizeof(buff_length) - 1);
}

#endif /* _WIN32 */


static thread_local char cached_thread_name[256]{};

int ceph_pthread_setname(char const* name)
{
  strncpy(cached_thread_name, name, sizeof cached_thread_name - 1);
#if defined(_WIN32) && defined(__clang__) && \
    !defined(_LIBCPP_HAS_THREAD_API_PTHREAD)
  // In this case, llvm doesn't use the pthread api for std::thread.
  // We cannot use native_handle() with the pthread api, nor can we pass
  // it to Windows API functions.
  return 0;
#elif defined(HAVE_PTHREAD_SETNAME_NP)
  #if defined(__APPLE__)
      return pthread_setname_np(name);
  #else
      return pthread_setname_np(pthread_self(), name);
  #endif
#elif defined(HAVE_PTHREAD_SET_NAME_NP)
  pthread_set_name_np(pthread_self(), name);          \
  return 0;
#else
  return 0;
#endif
}

int ceph_pthread_getname(char* name, size_t len)
{
  if (cached_thread_name[0]) {
    if (len > 0) {
      strncpy(name, cached_thread_name, len);
      name[len-1] = 0;
    }
    return 0;
  } else {
#if defined(_WIN32) && defined(__clang__) && \
    !defined(_LIBCPP_HAS_THREAD_API_PTHREAD)
    if (len > 0) {
      strcpy(name, "");
    }
    return 0;
#elif defined(HAVE_PTHREAD_GETNAME_NP) || defined(HAVE_PTHREAD_GET_NAME_NP)
#  if defined(HAVE_PTHREAD_GETNAME_NP)
    int rc = pthread_getname_np(pthread_self(), cached_thread_name, sizeof cached_thread_name);
#  else
    int rc = pthread_get_name_np(pthread_self(), cached_thread_name, sizeof cached_thread_name);
#  endif
    if (rc == 0) {
      strncpy(name, cached_thread_name, len);
      name[len-1] = 0;
      return 0;
    } else {
      return rc;
    }
#else
    if (len > 0) {
      strcpy(name, "");
    }
    return 0;
#endif
  }
}
