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
#include "acconfig.h"

#include "common/pipe.h"
#include "include/compat.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

int pipe_cloexec(int pipefd[2])
{
	int ret;

#if defined(HAVE_PIPE2) && defined(O_CLOEXEC)
	ret = pipe2(pipefd, O_CLOEXEC);
	if (ret == -1)
		return -errno;
	return 0;
#else
	ret = pipe(pipefd);
	if (ret == -1)
		return -errno;

	/*
	 * The old-fashioned, race-condition prone way that we have to fall
	 * back on if O_CLOEXEC does not exist.
	 */
	ret = fcntl(pipefd[0], F_SETFD, FD_CLOEXEC);
	if (ret == -1) {
		ret = -errno;
		goto out;
	}

	ret = fcntl(pipefd[1], F_SETFD, FD_CLOEXEC);
	if (ret == -1) {
		ret = -errno;
		goto out;
	}

	return 0;

out:
	TEMP_FAILURE_RETRY(close(pipefd[0]));
	TEMP_FAILURE_RETRY(close(pipefd[1]));

	return ret;
#endif
}
