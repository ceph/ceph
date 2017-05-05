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

class PGStatService {
public:
  PGStatService() {}
  virtual ~PGStatService() {}
  virtual void reset(const PGMap& o) = 0;
  virtual bool is_readable() const = 0;
  virtual const pool_stat_t* get_pool_stat(int poolid) const = 0;
  virtual const pool_stat_t& get_pg_sum() const = 0;
  virtual const osd_stat_t& get_osd_sum() const = 0;

  typedef ceph::unordered_map<pg_t,pg_stat_t>::const_iterator PGStatIter;
  typedef ceph::unordered_map<int32_t,osd_stat_t>::const_iterator OSDStatIter;
  virtual PGStatIter pg_stat_iter_begin() const = 0;
  virtual PGStatIter pg_stat_iter_end() const = 0;
  virtual OSDStatIter osd_stat_iter_begin() const = 0;
  virtual OSDStatIter osd_stat_iter_end() const = 0;
  virtual const osd_stat_t *get_osd_stat(int osd) const = 0;
  virtual float get_full_ratio() const = 0;
  virtual float get_nearfull_ratio() const = 0;
  virtual bool have_creating_pgs() const = 0;
  virtual bool is_creating_pg(pg_t pgid) const = 0;
  virtual epoch_t get_min_last_epoch_clean() const = 0;

  virtual PGMap& get_pg_map() = 0;

  virtual bool have_full_osds() const = 0;
  virtual bool have_nearfull_osds() const = 0;

  virtual size_t get_num_pg_by_osd(int osd) const = 0;
  virtual void print_summary(Formatter *f, ostream *out) const = 0;
  virtual void dump_fs_stats(stringstream *ss, Formatter *f, bool verbose) const = 0;
  virtual void dump_pool_stats(const OSDMap& osdm, stringstream *ss, Formatter *f,
			       bool verbose) const = 0;

  virtual int process_pg_command(const string& prefix,
				 const map<string,cmd_vartype>& cmdmap,
				 const OSDMap& osdmap,
				 Formatter *f,
				 stringstream *ss,
				 bufferlist *odata) = 0;

  virtual int reweight_by_utilization(const OSDMap &osd_map,
			      int oload,
			      double max_changef,
			      int max_osds,
			      bool by_pg, const set<int64_t> *pools,
			      bool no_increasing,
			      mempool::osdmap::map<int32_t, uint32_t>* new_weights,
			      std::stringstream *ss,
			      std::string *out_str,
			      Formatter *f) = 0;
};

class PGMapStatService : public PGMap, public PGStatService {
  PGMap& parent;
public:
  PGMapStatService() : PGMap(), PGStatService(),
		    parent(*static_cast<PGMap*>(this)) {}
  PGMapStatService(const PGMap& o) : PGMap(o), PGStatService(),
				  parent(*static_cast<PGMap*>(this)) {}
  void reset(const PGMap& o) {
    parent = o;
  }

  // FIXME: Kill this once we rip out PGMonitor post-luminous
  /** returns true if the underlying data is readable. Always true
   * post-luminous, but not when we are redirecting to the PGMonitor
   */
  bool is_readable() const { return true; }

  const pool_stat_t* get_pool_stat(int poolid) const {
    auto i = parent.pg_pool_sum.find(poolid);
    if (i != parent.pg_pool_sum.end()) {
      return &i->second;
    }
    return NULL;
  }

  PGMap& get_pg_map() { return parent; }

  const pool_stat_t& get_pg_sum() const { return parent.pg_sum; }
  const osd_stat_t& get_osd_sum() const { return parent.osd_sum; }

  PGStatIter pg_stat_iter_begin() const { return parent.pg_stat.begin(); }
  PGStatIter pg_stat_iter_end() const { return parent.pg_stat.end(); }
  OSDStatIter osd_stat_iter_begin() const { return parent.osd_stat.begin(); }
  OSDStatIter osd_stat_iter_end() const { return parent.osd_stat.end(); }
  const osd_stat_t *get_osd_stat(int osd) const {
    auto i = parent.osd_stat.find(osd);
    if (i == parent.osd_stat.end()) {
      return NULL;
    }
    return &i->second;
  }

  float get_full_ratio() const { return parent.full_ratio; }
  float get_nearfull_ratio() const { return parent.nearfull_ratio; }

  bool have_creating_pgs() const { return !parent.creating_pgs.empty(); }
  bool is_creating_pg(pg_t pgid) const { return parent.creating_pgs.count(pgid); }
  epoch_t get_min_last_epoch_clean() const { return parent.get_min_last_epoch_clean(); }

  bool have_full_osds() const { return !parent.full_osds.empty(); }
  bool have_nearfull_osds() const { return !parent.nearfull_osds.empty(); }

  size_t get_num_pg_by_osd(int osd) const { return parent.get_num_pg_by_osd(osd); }

  void print_summary(Formatter *f, ostream *out) const { parent.print_summary(f, out); }
  void dump_fs_stats(stringstream *ss, Formatter *f, bool verbose) const {
    parent.dump_fs_stats(ss, f, verbose);
  }
  void dump_pool_stats(const OSDMap& osdm, stringstream *ss, Formatter *f,
		       bool verbose) const {
    parent.dump_pool_stats(osdm, ss, f, verbose);
  }

  int process_pg_command(const string& prefix,
			 const map<string,cmd_vartype>& cmdmap,
			 const OSDMap& osdmap,
			 Formatter *f,
			 stringstream *ss,
			 bufferlist *odata) {
    return process_pg_map_command(prefix, cmdmap, parent, osdmap, f, ss, odata);
  }

  int reweight_by_utilization(const OSDMap &osd_map,
			      int oload,
			      double max_changef,
			      int max_osds,
			      bool by_pg, const set<int64_t> *pools,
			      bool no_increasing,
			      mempool::osdmap::map<int32_t, uint32_t>* new_weights,
			      std::stringstream *ss,
			      std::string *out_str,
			      Formatter *f) {
    return reweight::by_utilization(osd_map, parent, oload, max_changef,
				    max_osds, by_pg, pools, no_increasing,
				    new_weights, ss, out_str, f);
  }

};


#endif
