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

#if defined(__FreeBSD__)
#define	lseek64(fd, offset, whence)	lseek(fd, offset, whence)
#define	ENODATA	61
#define	MSG_MORE 0
#endif /* !__FreeBSD__ */

#ifndef TEMP_FAILURE_RETRY
#define TEMP_FAILURE_RETRY(expression) ({     \
  typeof(expression) __result;                \
  do {                                        \
    __result = (expression);                  \
  } while (__result == -1 && errno == EINTR); \
  __result; })
#endif

#endif /* !CEPH_COMPAT_H */
