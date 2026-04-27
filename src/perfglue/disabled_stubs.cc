// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include "common/LogClient.h"
#include "perfglue/cpu_profiler.h"

#include <vector>
#include <string>

void cpu_profiler_handle_command(const std::vector<std::string> &cmd,
				 std::ostream& out)
{
  out << "cpu_profiler support not linked in";
}
