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

#include "config.h"
#include "common/debug.h"
#include "common/errno.h"

#include <errno.h>
#include <stdarg.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#define dout_prefix *_dout

int run_cmd(const char *cmd, ...)
{
  int ret;
  std::vector <const char *> arr;
  va_list ap;
  va_start(ap, cmd);
  const char *c = cmd;
  do {
    arr.push_back(c);
    c = va_arg(ap, const char*);
  } while (c != NULL);
  va_end(ap);
  arr.push_back(NULL);

  ret = fork();
  if (ret == -1) {
    int err = errno;
    derr << "run_cmd(" << cmd << "): unable to fork(): " << cpp_strerror(err)
         << dendl;
    return -1;
  }
  else if (ret == 0) {
    // execvp doesn't modify its arguments, so the const-cast here is safe.
    execvp(cmd, (char * const*)&arr[0]);
    _exit(127);
  }
  int status;
  while (waitpid(ret, &status, 0) == -1) {
    int err = errno;
    if (err == EINTR)
      continue;
    derr << "run_cmd(" << cmd << "): waitpid error: "
	 << cpp_strerror(err) << dendl;
    return -1;
  }
  if (WIFEXITED(status)) {
    return WEXITSTATUS(status);
  }
  else if (WIFSIGNALED(status)) {
    derr << "run_cmd(" << cmd << "): terminated by signal" << dendl;
    return -1;
  }
  derr << "run_cmd(" << cmd << "): terminated by unknown mechanism" << dendl;
  return -1;
}
