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
 */
#ifndef HEAP_PROFILER_H_
#define HEAP_PROFILER_H_

#include <string>
#include <vector>
#include "common/config.h"

class LogClient;

/*
 * Ceph glue for the Google perftools heap profiler, included
 * as part of tcmalloc. This replaces ugly function pointers
 * and #ifdef hacks!
 */
bool ceph_using_tcmalloc();

/*
 * Configure the heap profiler
 */
void ceph_heap_profiler_init();

void ceph_heap_profiler_stats(char *buf, int length);

void ceph_heap_release_free_memory();

double ceph_heap_get_release_rate();

void ceph_heap_get_release_rate(double value);

bool ceph_heap_profiler_running();

void ceph_heap_profiler_start();

void ceph_heap_profiler_stop();

void ceph_heap_profiler_dump(const char *reason);

bool ceph_heap_get_numeric_property(const char *property, size_t *value);

bool ceph_heap_set_numeric_property(const char *property, size_t value);

void ceph_heap_profiler_handle_command(const std::vector<std::string> &cmd,
                                       ostream& out);

#endif /* HEAP_PROFILER_H_ */
