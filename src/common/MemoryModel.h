// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MEMORYMODEL_H
#define CEPH_MEMORYMODEL_H

#include <fstream>
#include <optional>
#include "include/common_fwd.h"
#include "include/compat.h"


class MemoryModel {
public:
  struct mem_snap_t {
    long peak;
    long size;
    long hwm;
    long rss;
    long data;
    long lib;

    long heap;

    mem_snap_t() : peak(0), size(0), hwm(0), rss(0), data(0), lib(0),
	     heap(0)
    {}

    long get_total() const { return size; }
    long get_rss() const { return rss; }
    long get_heap() const { return heap; }
  } last;

private:
  static inline constexpr const char* proc_stat_fn = PROCPREFIX "/proc/self/status";
  static inline constexpr const char* proc_maps_fn = PROCPREFIX "/proc/self/maps";

  std::ifstream proc_status{proc_stat_fn};
  std::ifstream proc_maps{proc_maps_fn};

  CephContext *cct;
  void _sample(mem_snap_t *p);
  std::optional<int64_t> get_mapped_heap();

public:
  explicit MemoryModel(CephContext *cct);
  void sample(mem_snap_t *p = 0) {
    _sample(&last);
    if (p)
      *p = last;
  }
};

#endif
