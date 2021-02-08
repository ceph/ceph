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

#include "include/compat.h"
#include "debug.h"
#include "errno.h"

#ifndef _WIN32
void dump_open_fds(CephContext *cct)
{
#ifdef __APPLE__
  const char *fn = "/dev/fd";
#else
  const char *fn = PROCPREFIX "/proc/self/fd";
#endif
  DIR *d = opendir(fn);
  if (!d) {
    lderr(cct) << "dump_open_fds unable to open " << fn << dendl;
    return;
  }
  struct dirent *de = nullptr;

  int n = 0;
  while ((de = ::readdir(d))) {
    if (de->d_name[0] == '.')
      continue;
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", fn, de->d_name);
    char target[PATH_MAX];
    ssize_t r = readlink(path, target, sizeof(target) - 1);
    if (r < 0) {
      r = -errno;
      lderr(cct) << "dump_open_fds unable to readlink " << path << ": " << cpp_strerror(r) << dendl;
      continue;
    }
    target[r] = 0;
    lderr(cct) << "dump_open_fds " << de->d_name << " -> " << target << dendl;
    n++;
  }
  lderr(cct) << "dump_open_fds dumped " << n << " open files" << dendl;

  closedir(d);
}
#else
void dump_open_fds(CephContext *cct)
{
}
#endif
