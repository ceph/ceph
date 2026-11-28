// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include "common/ceph_time.h"

#include "include/common_fwd.h"
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

void ceph_heap_release_to_system(size_t bytes);

/*
 * Tell tcmalloc that the calling thread is about to go idle, so that its
 * thread-local cache may be reclaimed. Rate limited by rgw_heap_release_interval:
 * if less than that has passed since 'last', nothing is released and 'last' is
 * returned. Otherwise the cache is released and the current time is returned.
 * An interval of 0 disables it.
 */
ceph::real_time ceph_heap_mark_thread_temporarily_idle(CephContext* cct,
                                                       ceph::real_time last);

double ceph_heap_get_release_rate();

void ceph_heap_get_release_rate(double value);

bool ceph_heap_profiler_running();

void ceph_heap_profiler_start();

void ceph_heap_profiler_stop();

void ceph_heap_profiler_dump(const char *reason);

bool ceph_heap_get_numeric_property(const char *property, size_t *value);

bool ceph_heap_set_numeric_property(const char *property, size_t value);

void ceph_heap_profiler_handle_command(const std::vector<std::string> &cmd,
                                       std::ostream& out);

#endif /* HEAP_PROFILER_H_ */
