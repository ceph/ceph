// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <unistd.h>
#include "common/hostname.h"

std::string ceph_get_hostname()
{
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX 255
#endif
  char buf[HOST_NAME_MAX + 1];
  gethostname(buf, sizeof(buf));
  return std::string(buf);
}

std::string ceph_get_short_hostname()
{
  std::string hostname = ceph_get_hostname();
  size_t pos = hostname.find('.');
  if (pos == std::string::npos)
  {
    return hostname;
  }
  else
  {
    return hostname.substr(0, pos);
  }
}
