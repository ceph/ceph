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

#include <dirent.h>
#include <errno.h>
#include <iostream>
#include <sstream>
#include <string.h>
#include <sys/types.h>

#include "common/debug.h"
#include "common/Thread.h"

/**
 * Return the number of threads in this process. The data is
 * retrieved from /proc and includes all threads, not just
 * "child" threads.
 * Behavior changed in 6fb416b083d518e5f524359cc3cacb66ccc63dca
 * to support eventual elimination of global variables.
 */
int Thread::get_num_threads(void)
{
  std::ostringstream oss;
  oss << "/proc/" << getpid() << "/task";
  DIR *dir = opendir(oss.str().c_str());
  if (!dir) {
    int err = errno;
    generic_dout(-1) << "is_multithreaded: failed to open '"
	<< oss.str() << "'" << dendl;
    return -err;
  }
  int num_entries = 0;
  while (true) {
    struct dirent *e = readdir(dir);
    if (!e)
      break;
    if ((strcmp(e->d_name, ".") == 0) ||
        (strcmp(e->d_name, "..") == 0))
      continue;
    num_entries++;
  }
  ::closedir(dir);
  if (num_entries == 0) {
    // Shouldn't happen.
    generic_dout(-1) << "is_multithreaded: no entries found in '"
	<< oss.str() << "'" << dendl;
    return -EINVAL;
  }
  return num_entries;
}
