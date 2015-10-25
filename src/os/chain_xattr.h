// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_OSD_CHAIN_XATTR_H
#define __CEPH_OSD_CHAIN_XATTR_H

#include "common/xattr.h"

#include <errno.h>

#if defined(__linux__)
#include <linux/limits.h>
#define CHAIN_XATTR_MAX_NAME_LEN ((XATTR_NAME_MAX + 1) / 2)
#elif defined(__APPLE__)
#include <sys/xattr.h>
#define CHAIN_XATTR_MAX_NAME_LEN ((XATTR_MAXNAMELEN + 1) / 2)
#else
#define CHAIN_XATTR_MAX_NAME_LEN  128
#endif

#define CHAIN_XATTR_MAX_BLOCK_LEN 2048

/*
 * XFS will only inline xattrs < 255 bytes, so for xattrs that are
 * likely to fit in the inode, stripe over short xattrs.
 */
#define CHAIN_XATTR_SHORT_BLOCK_LEN 250
#define CHAIN_XATTR_SHORT_LEN_THRESHOLD 1000

// wrappers to hide annoying errno handling.

static inline int sys_fgetxattr(int fd, const char *name, void *val, size_t size)
{
  int r = ::ceph_os_fgetxattr(fd, name, val, size);
  return (r < 0 ? -errno : r);
}
static inline int sys_getxattr(const char *fn, const char *name, void *val, size_t size)
{
  int r = ::ceph_os_getxattr(fn, name, val, size);
  return (r < 0 ? -errno : r);
}

static inline int sys_setxattr(const char *fn, const char *name, const void *val, size_t size)
{
  int r = ::ceph_os_setxattr(fn, name, val, size);
  return (r < 0 ? -errno : r);
}
static inline int sys_fsetxattr(int fd, const char *name, const void *val, size_t size)
{
  int r = ::ceph_os_fsetxattr(fd, name, val, size);
  return (r < 0 ? -errno : r);
}

static inline int sys_listxattr(const char *fn, char *names, size_t len)
{
  int r = ::ceph_os_listxattr(fn, names, len);
  return (r < 0 ? -errno : r);
}
static inline int sys_flistxattr(int fd, char *names, size_t len)
{
  int r = ::ceph_os_flistxattr(fd, names, len);
  return (r < 0 ? -errno : r);
}

static inline int sys_removexattr(const char *fn, const char *name)
{
  int r = ::ceph_os_removexattr(fn, name);
  return (r < 0 ? -errno : r);
}
static inline int sys_fremovexattr(int fd, const char *name)
{
  int r = ::ceph_os_fremovexattr(fd, name);
  return (r < 0 ? -errno : r);
}


// wrappers to chain large values across multiple xattrs

int chain_getxattr(const char *fn, const char *name, void *val, size_t size);
int chain_fgetxattr(int fd, const char *name, void *val, size_t size);
int chain_setxattr(const char *fn, const char *name, const void *val, size_t size, bool onechunk=false);
int chain_fsetxattr(int fd, const char *name, const void *val, size_t size, bool onechunk=false);
int chain_listxattr(const char *fn, char *names, size_t len);
int chain_flistxattr(int fd, char *names, size_t len);
int chain_removexattr(const char *fn, const char *name);
int chain_fremovexattr(int fd, const char *name);

#endif
