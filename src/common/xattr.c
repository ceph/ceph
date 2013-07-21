/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 Stanislav Sedov <stas@FreeBSD.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 * OSX Support
 * -----------
 * The xattr access routines on OSX include a `position` argument in addition
 * to the parameters of the standard Linux counterparts. A custom feature test
 * can be written to detect the different versions. For now we test for
 * __APPLE__, and default to the Linux version.
 */

#include "acconfig.h"

/*
 * The original FreeBSD port below (now guarded by HAVE_EXTATTR) was using
 * FreeBSD specific string manipulation routines.
 */
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#ifdef HAVE_SYS_EXTATTR_H
#include <sys/extattr.h>
#endif

#ifdef HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif

#ifdef HAVE_SYS_MALLOC_H
#include <sys/malloc.h>
#endif

#include "common/xattr.h"

/*
 * Sets extended attribute on a file.
 * Returns 0 on success, -1 on failure.
 */
int ceph_os_setxattr(const char *path, const char *name, const void *value,
		size_t size)
{
	int error;

#ifdef HAVE_EXTATTR
	error = extattr_set_file(path, EXTATTR_NAMESPACE_USER, name, value,
	    size);
	if (error > 0)
		error = 0;
#else
#ifdef __APPLE__
	error = setxattr(path, name, value, size, 0, 0);
#else
	error = setxattr(path, name, value, size, 0);
#endif
#endif

	return error;
}

int ceph_os_fsetxattr(int fd, const char *name, const void *value, size_t size)
{
	int error;

#ifdef HAVE_EXTATTR
	error = extattr_set_fd(fd, EXTATTR_NAMESPACE_USER, name, value,
	    size);
	if (error > 0)
		error = 0;
#else
#ifdef __APPLE__
	error = fsetxattr(fd, name, value, size, 0, 0);
#else
	error = fsetxattr(fd, name, value, size, 0);
#endif
#endif

	return error;
}

ssize_t ceph_os_getxattr(const char *path, const char *name, void *value,
		size_t size)
{
	ssize_t error;

#ifdef HAVE_EXTATTR
	if (value == NULL || size == 0) {
		error = extattr_get_file(path, EXTATTR_NAMESPACE_USER, name, value,
		    size);
	} else {
		error = extattr_get_file(path, EXTATTR_NAMESPACE_USER, name, NULL,
		    0);
		if (error > 0) {
			if (error > size) {
				errno = ERANGE;
				error = -1;
			} else  {
				error = extattr_get_file(path, EXTATTR_NAMESPACE_USER,
				    name, value, size);
			}
		}
	}
#else
#ifdef __APPLE__
	error = getxattr(path, name, value, size, 0, 0);
#else
	error = getxattr(path, name, value, size);
#endif
#endif

	return error;
}

ssize_t ceph_os_fgetxattr(int fd, const char *name, void *value, size_t size)
{
	ssize_t error;

#ifdef HAVE_EXTATTR
	if (value == NULL || size == 0) {
		error = extattr_get_fd(fd, EXTATTR_NAMESPACE_USER, name, value,
		    size);
	} else {
		error = extattr_get_fd(fd, EXTATTR_NAMESPACE_USER, name, NULL,
		    0);
		if (error > 0) {
			if (error > size) {
				errno = ERANGE;
				error = -1;
			} else  {
				error = extattr_get_fd(fd, EXTATTR_NAMESPACE_USER,
				    name, value, size);
			}
		}
	}
#else
#ifdef __APPLE__
	error = fgetxattr(fd, name, value, size, 0, 0);
#else
	error = fgetxattr(fd, name, value, size);
#endif
#endif

	return error;
}

ssize_t ceph_os_listxattr(const char *path, char *list, size_t size)
{
	ssize_t error;

#ifdef HAVE_EXTATTR
	/*
	 * XXX. The format of the list FreeBSD returns differs
	 * from the Linux ones.  We have to perform the conversion. :-(
	 */
	char *newlist, *p, *p1;

	if (size != 0) {
		newlist = malloc(size);
		if (newlist != NULL) {
			error = extattr_list_file(path, EXTATTR_NAMESPACE_USER,
			    newlist, size);
			if (error > 0) {
				p = newlist;
				p1 = list;
				while ((p - newlist) < error) {
					uint8_t len = *(uint8_t *)p;
					p++;
					if ((p + len - newlist) > error)
						break;
					if (len > 0) {
						bcopy(p, p1, len);
						p += len;
						p1 += len;
						*p1++ = '\0';
					}
				}
				error = p1 - list;
			}
			free(newlist);
		}
	} else {
		error = extattr_list_file(path, EXTATTR_NAMESPACE_USER,
		    list, size);
	}
#else
#ifdef __APPLE__
	error = listxattr(path, list, size, 0);
#else
	error = listxattr(path, list, size);
#endif
#endif

	return error;
}

ssize_t ceph_os_flistxattr(int fd, char *list, size_t size)
{
	ssize_t error;

#ifdef HAVE_EXTATTR
	/*
	 * XXX. The format of the list FreeBSD returns differs
	 * from the Linux ones.  We have to perform the conversion. :-(
	 */
	char *newlist, *p, *p1;

	if (size != 0) {
		newlist = malloc(size);
		if (newlist != NULL) {
			error = extattr_list_fd(fd, EXTATTR_NAMESPACE_USER,
			    newlist, size);
			if (error > 0) {
				p = newlist;
				p1 = list;
				while ((p - newlist) < error) {
					uint8_t len = *(uint8_t *)p;
					p++;
					if ((p + len - newlist) > error)
						break;
					if (len > 0) {
						bcopy(p, p1, len);
						p += len;
						p1 += len;
						*p1++ = '\0';
					}
				}
				error = p1 - list;
			}
			free(newlist);
		}
	} else {
		error = extattr_list_fd(fd, EXTATTR_NAMESPACE_USER,
		    list, size);
	}
#else
#ifdef __APPLE__
	error = flistxattr(fd, list, size, 0);
#else
	error = flistxattr(fd, list, size);
#endif
#endif

	return error;
}

int ceph_os_removexattr(const char *path, const char *name)
{
	int error;

#ifdef HAVE_EXTATTR
	error = extattr_delete_file(path, EXTATTR_NAMESPACE_USER, name);
#else
#ifdef __APPLE__
	error = removexattr(path, name, 0);
#else
	error = removexattr(path, name);
#endif
#endif

	return error;
}

int ceph_os_fremovexattr(int fd, const char *name)
{
	int error;

#ifdef HAVE_EXTATTR
	error = extattr_delete_fd(fd, EXTATTR_NAMESPACE_USER, name);
#else
#ifdef __APPLE__
	error = fremovexattr(fd, name, 0);
#else
	error = fremovexattr(fd, name);
#endif
#endif

	return error;
}
