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
#include <acconfig.h>

#include "common/pipe_util.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include <sys/fcntl.h> /* O_CLOEXEC on OSX 10.8 */

int pipe_cloexec(int pipefd[2])
{
	int ret;

#ifdef HAVE_PIPE2
#ifdef O_CLOEXEC
	ret = pipe2(pipefd, O_CLOEXEC);
#else
	ret = pipe2(pipefd, 0);
#endif
#else
	ret = pipe(pipefd);
#endif
	if (ret)
		return -errno;

#if !defined(HAVE_PIPE2) || !defined(O_CLOEXEC)
	/*
	 * The old-fashioned, race-condition prone way that we have to fall
	 * back on if O_CLOEXEC does not exist.
	 */
	fcntl(pipefd[0], F_SETFD, FD_CLOEXEC);
	fcntl(pipefd[1], F_SETFD, FD_CLOEXEC);
#endif

	return 0;
}
