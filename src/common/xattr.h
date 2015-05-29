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

#ifndef CEPH_EXTATTR_H
#define CEPH_EXTATTR_H

#include <sys/types.h>
#include <errno.h>

#ifdef __cplusplus
extern "C" {
#endif

// Almost everyone defines ENOATTR, except for Linux,
// which does #define ENOATTR ENODATA.  It seems that occasionally that
// isn't defined, though, so let's make sure.
#ifndef ENOATTR
# define ENOATTR ENODATA
#endif

int ceph_os_setxattr(const char *path, const char *name,
                  const void *value, size_t size);
int ceph_os_fsetxattr(int fd, const char *name, const void *value,
                   size_t size);
ssize_t ceph_os_getxattr(const char *path, const char *name,
                         void *value, size_t size);
ssize_t ceph_os_fgetxattr(int fd, const char *name, void *value,
                          size_t size);
ssize_t ceph_os_listxattr(const char *path, char *list, size_t size);
ssize_t ceph_os_flistxattr(int fd, char *list, size_t size);
int ceph_os_removexattr(const char *path, const char *name);
int ceph_os_fremovexattr(int fd, const char *name);

#ifdef __cplusplus
}
#endif

#endif /* !CEPH_EXTATTR_H */
