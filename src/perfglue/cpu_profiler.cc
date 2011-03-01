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

#include "common/LogClient.h"
#include "perfglue/cpu_profiler.h"

#include <google/profiler.h>

void cpu_profiler_handle_command(const std::vector<std::string> &cmd,
				 LogClient &clog)
{
  if (cmd[1] == "start") {
    if (cmd.size() < 3) {
      clog.info() << "cpu_profiler: you must give an argument to start: a "
	          << "file name to log to.\n";
      return;
    }
    const char *file = cmd[2].c_str();
    int r = ProfilerStart(file);
    clog.info() << "cpu_profiler: starting logging to " << file
		<< ", r=" << r << "\n";
  }
  else if (cmd[1] == "status") {
    if (ProfilingIsEnabledForAllThreads())
      clog.info() << "cpu_profiler is enabled\n";
    else
      clog.info() << "cpu_profiler is not enabled\n";
    ProfilerState st;
    ProfilerGetCurrentState(&st);
    clog.info() << "cpu_profiler " << (st.enabled ? "enabled":"not enabled")
		<< " start_time " << st.start_time
		<< " profile_name " << st.profile_name
		<< " samples " << st.samples_gathered
		<< "\n";
  }
  else if (cmd[1] == "flush") {
    clog.info() << "cpu_profiler: flushing\n";
    ProfilerFlush();
  }
  else if (cmd[1] == "stop") {
    clog.info() << "cpu_profiler: flushing and stopping\n";
    ProfilerFlush();
    ProfilerStop();
  }
  else {
    clog.info() << "can't understand cpu_profiler command. Expected one of: "
		<< "start,status,flush,stop.\n";
  }
}
