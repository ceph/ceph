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
#include "heap_profiler.h"

bool ceph_using_tcmalloc() { return false; }

void ceph_heap_profiler_init() { return; }

void ceph_heap_profiler_stats(char *buf, int length) { return; }

void ceph_heap_release_free_memory() { return; }

bool ceph_heap_profiler_running() { return false; }

void ceph_heap_profiler_start() { return; }

void ceph_heap_profiler_stop() { return; }

void ceph_heap_profiler_dump(const char *reason) { return; }

bool ceph_heap_get_numeric_property(const char *property, size_t *value)
{
  return false;
}

bool ceph_heap_set_numeric_property(const char *property, size_t value)
{
  return false;
}

void ceph_heap_profiler_handle_command(const std::vector<std::string>& cmd,
                                       ostream& out) { return; }
