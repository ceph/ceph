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
#ifndef CEPH_PERFGLUE_CPU_PROFILER

/*
 * Ceph glue for the Google Perftools CPU profiler
 */
#include <string>
#include <vector>

void cpu_profiler_handle_command(const std::vector<std::string> &cmd,
				 ostream& out);

#endif
