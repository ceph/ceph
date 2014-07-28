// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network/Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include "heap_profiler.h"
#include "common/environment.h"
#include "common/LogClient.h"
#include "global/global_context.h"
#include "common/debug.h"

bool ceph_using_tcmalloc()
{
  return true;
}

void ceph_heap_profiler_init()
{
  // Two other interesting environment variables to set are:
  // HEAP_PROFILE_ALLOCATION_INTERVAL, HEAP_PROFILE_INUSE_INTERVAL
  if (get_env_bool("CEPH_HEAP_PROFILER_INIT")) {
    ceph_heap_profiler_start();
  }
}

void ceph_heap_profiler_stats(char *buf, int length)
{
  MallocExtension::instance()->GetStats(buf, length);
}

void ceph_heap_release_free_memory()
{
  MallocExtension::instance()->ReleaseFreeMemory();
}

bool ceph_heap_profiler_running()
{
  return false;
}

void ceph_heap_profiler_start()
{
}

void ceph_heap_profiler_stop()
{
}

void ceph_heap_profiler_dump(const char *reason)
{
}

void ceph_heap_profiler_handle_command(const std::vector<std::string>& cmd,
                                       ostream& out)
{
  if (cmd.size() == 1 && cmd[0] == "release") {
    ceph_heap_release_free_memory();
    out << g_conf->name << " releasing free RAM back to system.";
  } else if (cmd.size() == 1 && cmd[0] == "stats") {
    char *heap_stats = new char[1024];
    ceph_heap_profiler_stats(heap_stats, 1024);
    out << g_conf->name << "tcmalloc heap stats:"
	<< heap_stats;
  } else {
    out << "unknown command " << cmd;
  }
}
