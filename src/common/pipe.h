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

#ifndef CEPH_COMMON_PIPE_H
#define CEPH_COMMON_PIPE_H

#ifdef __cplusplus
extern "C" {
#endif

/** Create a pipe and set both ends to have F_CLOEXEC
 *
 * @param pipefd	pipe array, just as in pipe(2)
 * @return		0 on success, errno otherwise 
 */
int pipe_cloexec(int pipefd[2]);

#ifdef __cplusplus
}
#endif

#endif
