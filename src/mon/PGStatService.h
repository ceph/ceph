// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Greg Farnum/Red Hat <gfarnum@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/**
 * This service abstracts out the specific implementation providing information
 * needed by parts of the Monitor based around PGStats. This'll make for
 * an easier transition from the PGMonitor-based queries where we handle
 * PGStats directly, to where we are getting information passed in from
 * the Ceph Manager.
 *
 * This initial implementation cheats by wrapping a PGMap so we don't need
 * to reimplement everything in one go.
 */

#ifndef CEPH_PGSTATSERVICE_H
#define CEPH_PGSTATSERVICE_H

#include "mon/PGMap.h"

class PGStatService : public PGMap {
  PGMap& parent;
public:
  PGStatService() : PGMap(),
		    parent(*static_cast<PGMap*>(this)) {}
  PGStatService(const PGMap& o) : PGMap(o),
				  parent(*static_cast<PGMap*>(this)) {}
  PGStatService& operator=(const PGMap& o) {
    reset(o);
    return *this;
  }
  void reset(const PGMap& o) {
    parent = o;
  }

  /** returns true if the underlying data is readable. Always true
   * post-luminous, but not when we are redirecting to the PGMonitor
   */
  bool is_readable() { return true; }

  const pool_stat_t* get_pool_stat(int poolid) const {
    auto i = parent.pg_pool_sum.find(poolid);
    if (i != parent.pg_pool_sum.end()) {
      return &i->second;
    }
    return NULL;
  }

  PGMap& get_pg_map() { return parent; }

  float get_full_ratio() const { return parent.full_ratio; }
  float get_nearfull_ratio() const { return parent.nearfull_ratio; }

  bool have_creating_pgs() const { return !parent.creating_pgs.empty(); }
  bool is_creating_pg(pg_t pgid) const { return parent.creating_pgs.count(pgid); }
  epoch_t get_min_last_epoch_clean() const { return parent.get_min_last_epoch_clean(); }

  bool have_full_osds() const { return !parent.full_osds.empty(); }
  bool have_nearfull_osds() const { return !parent.nearfull_osds.empty(); }
};


#endif
