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

#include "common/pipe.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

int pipe_cloexec(int pipefd[2])
{
#if defined(O_CLOEXEC) && !defined(__FreeBSD__)
	int ret;
	ret = pipe2(pipefd, O_CLOEXEC);
	if (ret) {
		ret = -errno;
		return ret;
	}
	return 0;
#else
	/* The old-fashioned, race-condition prone way that we have to fall back on if
	 * O_CLOEXEC does not exist. */
	int ret = pipe(pipefd);
	if (ret) {
		ret = -errno;
		return ret;
	}
	fcntl(pipefd[0], F_SETFD, FD_CLOEXEC);
	fcntl(pipefd[1], F_SETFD, FD_CLOEXEC);
	return 0;
#endif
}
