// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#define _XOPEN_SOURCE 500

#include <unistd.h>
#include <errno.h>

#include "common/safe_io.h"

ssize_t safe_read(int fd, void *buf, size_t count)
{
	size_t cnt = 0;

	while (cnt < count) {
		ssize_t r = read(fd, buf, count - cnt);
		if (r <= 0) {
			if (r == 0) {
				// EOF
				return cnt;
			}
			if (errno == EINTR)
				continue;
			return -errno;
		}
		cnt += r;
		buf = (char *)buf + r;
	}
	return cnt;
}

ssize_t safe_read_exact(int fd, void *buf, size_t count)
{
        ssize_t ret = safe_read(fd, buf, count);
	if (ret < 0)
		return ret;
	if ((size_t)ret != count)
		return -EDOM;
	return 0;
}
 
ssize_t safe_write(int fd, const void *buf, size_t count)
{
	while (count > 0) {
		ssize_t r = write(fd, buf, count);
		if (r < 0) {
			if (errno == EINTR)
				continue;
			return -errno;
		}
		count -= r;
		buf = (char *)buf + r;
	}
	return 0;
}

ssize_t safe_pread(int fd, void *buf, size_t count, off_t offset)
{
	size_t cnt = 0;
	char *b = (char*)buf;

	while (cnt < count) {
		ssize_t r = pread(fd, b + cnt, count - cnt, offset + cnt);
		if (r <= 0) {
			if (r == 0) {
				// EOF
				return cnt;
			}
			if (errno == EINTR)
				continue;
			return -errno;
		}

		cnt += r;
	}
	return cnt;
}

ssize_t safe_pread_exact(int fd, void *buf, size_t count, off_t offset)
{
	ssize_t ret = safe_pread(fd, buf, count, offset);
	if (ret < 0)
		return ret;
	if ((size_t)ret != count)
		return -EDOM;
	return 0;
}

ssize_t safe_pwrite(int fd, const void *buf, size_t count, off_t offset)
{
	while (count > 0) {
		ssize_t r = pwrite(fd, buf, count, offset);
		if (r < 0) {
			if (errno == EINTR)
				continue;
			return -errno;
		}
		count -= r;
		buf = (char *)buf + r;
		offset += r;
	}
	return 0;
}
