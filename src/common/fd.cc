// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "fd.h"

#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>

#include "debug.h"
#include "errno.h"

void dump_open_fds(CephContext *cct)
{
  const char *fn = "/proc/self/fd";
  DIR *d = opendir(fn);
  if (!d) {
    lderr(cct) << "dump_open_fds unable to open " << fn << dendl;
    return;
  }
  struct dirent de, *pde = 0;

  int n = 0;
  while (readdir_r(d, &de, &pde) >= 0) {
    if (pde == NULL)
      break;
    if (de.d_name[0] == '.')
      continue;
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", fn, de.d_name);
    char target[PATH_MAX];
    ssize_t r = readlink(path, target, sizeof(target) - 1);
    if (r < 0) {
      r = -errno;
      lderr(cct) << "dump_open_fds unable to readlink " << path << ": " << cpp_strerror(r) << dendl;
      continue;
    }
    target[r] = 0;
    lderr(cct) << "dump_open_fds " << de.d_name << " -> " << target << dendl;
    n++;
  }
  lderr(cct) << "dump_open_fds dumped " << n << " open files" << dendl;

  closedir(d);
}
