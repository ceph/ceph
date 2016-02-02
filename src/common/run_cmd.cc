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

#include "common/errno.h"

#include <errno.h>
#include <sstream>
#include <stdarg.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

using std::ostringstream;

std::string run_cmd(const char *cmd, ...)
{
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

  int fret = fork();
  if (fret == -1) {
    int err = errno;
    ostringstream oss;
    oss << "run_cmd(" << cmd << "): unable to fork(): " << cpp_strerror(err);
    return oss.str();
  }
  else if (fret == 0) {
    // execvp doesn't modify its arguments, so the const-cast here is safe.
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
    execvp(cmd, (char * const*)&arr[0]);
    _exit(127);
  }
  int status;
  while (waitpid(fret, &status, 0) == -1) {
    int err = errno;
    if (err == EINTR)
      continue;
    ostringstream oss;
    oss << "run_cmd(" << cmd << "): waitpid error: "
	 << cpp_strerror(err);
    return oss.str();
  }
  if (WIFEXITED(status)) {
    int wexitstatus = WEXITSTATUS(status);
    if (wexitstatus != 0) {
      ostringstream oss;
      oss << "run_cmd(" << cmd << "): exited with status " << wexitstatus;
      return oss.str();
    }
    return "";
  }
  else if (WIFSIGNALED(status)) {
    ostringstream oss;
    oss << "run_cmd(" << cmd << "): terminated by signal";
    return oss.str();
  }
  ostringstream oss;
  oss << "run_cmd(" << cmd << "): terminated by unknown mechanism";
  return oss.str();
}
