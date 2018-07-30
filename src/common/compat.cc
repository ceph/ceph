// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <sys/mount.h>
#if defined(__linux__) 
#include <sys/vfs.h>
#endif

#include "include/compat.h" 
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

