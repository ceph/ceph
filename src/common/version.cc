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
#include "ceph_ver.h"
#include "common/debug.h"
#include "common/version.h"

#include <sstream>
#include <string>

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

std::string ceph_version_to_string(void)
{
  std::ostringstream oss;
  oss << "ceph version " << VERSION << " (commit:"
      << STRINGIFY(CEPH_GIT_VER) << ")";
  return oss.str();
}

void dout_output_ceph_version(void)
{
  generic_dout(-1) << ceph_version_to_string() << dendl;
}
