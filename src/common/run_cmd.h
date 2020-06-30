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

#ifndef CEPH_COMMON_RUN_CMD_H
#define CEPH_COMMON_RUN_CMD_H

#include <string>

//
// Fork a command and run it. The shell will not be invoked and shell
// expansions will not be done.
// This function takes a variable number of arguments. The last argument must
// be NULL.
//
// Example:
//   run_cmd("rm", "-rf", "foo", NULL)
//
// Returns an empty string on success, and an error string otherwise.
//
std::string run_cmd(const char *cmd, ...);

#endif
