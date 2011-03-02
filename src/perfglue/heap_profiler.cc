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

#include <google/heap-profiler.h>
#include <google/malloc_extension.h>
#include "heap_profiler.h"
#include "common/LogClient.h"

bool ceph_using_tcmalloc()
{
  return true;
}

void ceph_heap_profiler_init()
{
  char profile_name[PATH_MAX];
  sprintf(profile_name, "%s/%s", g_conf.log_dir, g_conf.name);
  char *val = new char[sizeof(int)*8+1];
  sprintf(val, "%i", g_conf.profiler_allocation_interval);
  setenv("HEAP_PROFILE_ALLOCATION_INTERVAL", val, g_conf.profiler_allocation_interval);
  sprintf(val, "%i", g_conf.profiler_highwater_interval);
  setenv("HEAP_PROFILE_INUSE_INTERVAL", val, g_conf.profiler_highwater_interval);
  if (g_conf.tcmalloc_profiler_run) {
    generic_dout(0) << "turning on heap profiler with prefix " << profile_name << dendl;
    HeapProfilerStart(profile_name);
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
  return IsHeapProfilerRunning();
}

void ceph_heap_profiler_start()
{
  char profile_name[PATH_MAX];
  sprintf(profile_name, "%s/%s", g_conf.log_dir, g_conf.name);
  char *val = new char[sizeof(int)*8+1];
  sprintf(val, "%i", g_conf.profiler_allocation_interval);
  setenv("HEAP_PROFILE_ALLOCATION_INTERVAL", val, g_conf.profiler_allocation_interval);
  sprintf(val, "%i", g_conf.profiler_highwater_interval);
  setenv("HEAP_PROFILE_INUSE_INTERVAL", val, g_conf.profiler_highwater_interval);
  generic_dout(0) << "turning on heap profiler with prefix " << profile_name << dendl;
  HeapProfilerStart(profile_name);
}

void ceph_heap_profiler_stop()
{
  HeapProfilerStop();
}

void ceph_heap_profiler_dump(const char *reason)
{
  HeapProfilerDump(reason);
}

void ceph_heap_profiler_handle_command(const std::vector<std::string>& cmd,
                                       LogClient &clog)
{
  if (cmd.size() == 2 && cmd[1] == "dump") {
    char *heap_stats = new char[1024];
    ceph_heap_profiler_stats(heap_stats, 1024);
    clog.info() << g_conf.name << "dumping heap profile now.\n"
        << heap_stats << std::endl;
    ceph_heap_profiler_dump("admin request");
  } else if (cmd.size() == 2 && cmd[1] == "export_config") {
    char val[sizeof(int)*8+1];
    snprintf(val, sizeof(val), "%i", g_conf.profiler_allocation_interval);
    setenv("HEAP_PROFILE_ALLOCATION_INTERVAL", val, g_conf.profiler_allocation_interval);
    snprintf(val, sizeof(val), "%i", g_conf.profiler_highwater_interval);
    setenv("HEAP_PROFILE_INUSE_INTERVAL", val, g_conf.profiler_highwater_interval);
    clog.info() << g_conf.name << " set heap variables from current config";
  } else if (cmd.size() == 2 && cmd[1] == "start_profiler") {
    ceph_heap_profiler_start();
    clog.info() << g_conf.name << " started profiler \n";
  } else if (cmd.size() == 2 && cmd[1] == "stop_profiler") {
    ceph_heap_profiler_stop();
    clog.info() << g_conf.name << " stopped profiler\n";
  } else if (cmd.size() == 2 && cmd[1] == "release") {
    ceph_heap_release_free_memory();
    clog.info() << g_conf.name << " releasing free RAM back to system.\n";
  } else if (cmd.size() == 2 && cmd[1] == "stats") {
    char *heap_stats = new char[1024];
    ceph_heap_profiler_stats(heap_stats, 1024);
    clog.info() << g_conf.name << "tcmalloc heap stats:" << heap_stats << std::endl;
  } else {
    clog.warn() << "unknown command " << cmd << std::endl;
  }
}
