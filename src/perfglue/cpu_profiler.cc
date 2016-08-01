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

#include "acconfig.h"

// Use the newer gperftools header locations if available.
// If not, fall back to the old (gperftools < 2.0) locations.

#ifdef HAVE_GPERFTOOLS_PROFILER_H
  #include <gperftools/profiler.h>
#else
  #include <google/profiler.h>
#endif

#include "common/LogClient.h"
#include "perfglue/cpu_profiler.h"

void cpu_profiler_handle_command(const std::vector<std::string> &cmd,
				 ostream& out)
{
  if (cmd[1] == "status") {
    ProfilerState st;
    ProfilerGetCurrentState(&st);
    out << "cpu_profiler " << (st.enabled ? "enabled":"not enabled")
	<< " start_time " << st.start_time
	<< " profile_name " << st.profile_name
	<< " samples " << st.samples_gathered;
  }
  else if (cmd[1] == "flush") {
    ProfilerFlush();
    out << "cpu_profiler: flushed";
  }
  else {
    out << "cpu_profiler: unrecognized command " << cmd
	<< "; expected one of status, flush.";
  }
}
