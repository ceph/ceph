// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sys/types.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/syscall.h>   /* For SYS_xxx definitions */
#endif
#include <algorithm>
#include <errno.h>

#include "common/errno.h"
#include "io_priority.h"

pid_t ceph_gettid(void)
{
#ifdef __linux__
  return syscall(SYS_gettid);
#else
  return -ENOSYS;
#endif
}

int ceph_ioprio_set(int whence, int who, int ioprio)
{
#ifdef __linux__
  return syscall(SYS_ioprio_set, whence, who, ioprio);
#else
  return -ENOSYS;
#endif
}

int ceph_ioprio_string_to_class(const std::string& s)
{
  std::string l = s;
  std::transform(l.begin(), l.end(), l.begin(), ::tolower);

  if (l == "idle")
    return IOPRIO_CLASS_IDLE;
  if (l == "be" || l == "besteffort" || l == "best effort")
    return IOPRIO_CLASS_BE;
  if (l == "rt" || l == "realtime" || l == "real time")
    return IOPRIO_CLASS_RT;
  return -EINVAL;
}
