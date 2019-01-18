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

#include "acconfig.h"

// Use the newer gperftools header locations if available.
// If not, fall back to the old (gperftools < 2.0) locations.

#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>

#include "heap_profiler.h"
#include "common/environment.h"
#include "common/LogClient.h"
#include "global/global_context.h"
#include "common/debug.h"

#define dout_context g_ceph_context

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

double ceph_heap_get_release_rate()
{
  return MallocExtension::instance()->GetMemoryReleaseRate();
}

void ceph_heap_set_release_rate(double val)
{
  MallocExtension::instance()->SetMemoryReleaseRate(val);
}

bool ceph_heap_get_numeric_property(
  const char *property, size_t *value)
{
  return MallocExtension::instance()->GetNumericProperty(
    property,
    value);
}

bool ceph_heap_set_numeric_property(
  const char *property, size_t value)
{
  return MallocExtension::instance()->SetNumericProperty(
    property,
    value);
}

bool ceph_heap_profiler_running()
{
#ifdef HAVE_LIBTCMALLOC
  return IsHeapProfilerRunning();
#else
  return false;
#endif
}

static void get_profile_name(char *profile_name, int profile_name_len)
{
#if __GNUC__ && __GNUC__ >= 8
#pragma GCC diagnostic push
  // Don't care, it doesn't matter, and we can't do anything about it.
#pragma GCC diagnostic ignored "-Wformat-truncation"
#endif

  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s", g_conf()->log_file.c_str());
  char *last_slash = rindex(path, '/');

  if (last_slash == NULL) {
    snprintf(profile_name, profile_name_len, "./%s.profile",
	     g_conf()->name.to_cstr());
  }
  else {
    last_slash[1] = '\0';
    snprintf(profile_name, profile_name_len, "%s/%s.profile",
	     path, g_conf()->name.to_cstr());
  }
#if __GNUC__ && __GNUC__ >= 8
#pragma GCC diagnostic pop
#endif
}

void ceph_heap_profiler_start()
{
#ifdef HAVE_LIBTCMALLOC
  char profile_name[PATH_MAX];
  get_profile_name(profile_name, sizeof(profile_name)); 
  generic_dout(0) << "turning on heap profiler with prefix "
		  << profile_name << dendl;
  HeapProfilerStart(profile_name);
#endif
}

void ceph_heap_profiler_stop()
{
#ifdef HAVE_LIBTCMALLOC
  HeapProfilerStop();
#endif
}

void ceph_heap_profiler_dump(const char *reason)
{
#ifdef HAVE_LIBTCMALLOC
  HeapProfilerDump(reason);
#endif
}

#define HEAP_PROFILER_STATS_SIZE 2048

void ceph_heap_profiler_handle_command(const std::vector<std::string>& cmd,
                                       ostream& out)
{
#ifdef HAVE_LIBTCMALLOC
  if (cmd.size() == 1 && cmd[0] == "dump") {
    if (!ceph_heap_profiler_running()) {
      out << "heap profiler not running; can't dump";
      return;
    }
    char heap_stats[HEAP_PROFILER_STATS_SIZE];
    ceph_heap_profiler_stats(heap_stats, sizeof(heap_stats));
    out << g_conf()->name << " dumping heap profile now.\n"
	<< heap_stats;
    ceph_heap_profiler_dump("admin request");
  } else if (cmd.size() == 1 && cmd[0] == "start_profiler") {
    ceph_heap_profiler_start();
    out << g_conf()->name << " started profiler";
  } else if (cmd.size() == 1 && cmd[0] == "stop_profiler") {
    ceph_heap_profiler_stop();
    out << g_conf()->name << " stopped profiler";
  } else if (cmd.size() == 1 && cmd[0] == "release") {
    ceph_heap_release_free_memory();
    out << g_conf()->name << " releasing free RAM back to system.";
  } else if (cmd.size() == 1 && cmd[0] == "get_release_rate") {
    out << g_conf()->name << " release rate: " 
	<< std::setprecision(4) << ceph_heap_get_release_rate() << "\n";
  } else if (cmd.size() == 2 && cmd[0] == "set_release_rate") {
    try {
      double val = std::stod(cmd[1]);
      ceph_heap_set_release_rate(val);
      out << g_conf()->name <<  " release rate changed to: " 
          << std::setprecision(4) << ceph_heap_get_release_rate() << "\n";
    } catch (...) {
      out << g_conf()->name <<  " *** need an numerical value. ";
    }
  } else
#endif
  if (cmd.size() == 1 && cmd[0] == "stats") {
    char heap_stats[HEAP_PROFILER_STATS_SIZE];
    ceph_heap_profiler_stats(heap_stats, sizeof(heap_stats));
    out << g_conf()->name << " tcmalloc heap stats:"
	<< heap_stats;
  } else {
    out << "unknown command " << cmd;
  }
}
