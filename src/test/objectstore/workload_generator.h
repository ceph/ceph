// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */
#ifndef WORKLOAD_GENERATOR_H_
#define WORKLOAD_GENERATOR_H_

#include "os/ObjectStore.h"
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <map>
#include <sys/time.h>

#include "TestObjectStoreState.h"

typedef boost::mt11213b rngen_t;

class WorkloadGenerator : public TestObjectStoreState {
 public:
  static const int def_max_in_flight = 50;

  static const int def_destroy_coll_every_nr_runs = 100;
  static const int def_num_obj_per_coll = 6000;
  static const int def_num_colls = 30;

  static const size_t min_write_bytes = 1;
  static const size_t max_write_mb = 5;
  static const size_t max_write_bytes = (max_write_mb * 1024 * 1024);

  static const size_t min_xattr_obj_bytes = 2;
  static const size_t max_xattr_obj_bytes = 300;
  static const size_t min_xattr_coll_bytes = 4;
  static const size_t max_xattr_coll_bytes = 600;

  static const size_t log_append_bytes = 1024;

  struct C_StatState {
    utime_t start;
    unsigned int written_data;
    WorkloadGenerator *wrkldgen;

    C_StatState(WorkloadGenerator *state, utime_t s)
      : start(s), written_data(0), wrkldgen(state) { }
  };


 protected:
  int m_max_in_flight;
  int m_num_ops;
  int m_destroy_coll_every_nr_runs;
  atomic_t m_nr_runs;

  int m_num_colls;

  rngen_t m_rng;

  map<coll_t, uint64_t> pg_log_size;

  size_t m_write_data_bytes;
  size_t m_write_xattr_obj_bytes;
  size_t m_write_xattr_coll_bytes;
  size_t m_write_pglog_bytes;

  bool m_suppress_write_data;
  bool m_suppress_write_xattr_obj;
  bool m_suppress_write_xattr_coll;
  bool m_suppress_write_log;

  bool m_do_stats;

  int m_stats_finished_txs;
  Mutex m_stats_lock;
  int m_stats_show_secs;

  size_t m_stats_total_written;
  utime_t m_stats_begin;

 private:

  void _suppress_ops_or_die(std::string& val);
  size_t _parse_size_or_die(std::string& val);
  void init_args(vector<const char*> args);

  int get_uniform_random_value(int min, int max);
  coll_entry_t *get_rnd_coll_entry(bool erase);
  hobject_t *get_rnd_obj(coll_entry_t *entry);
  int get_random_collection_nr();
  int get_random_object_nr(int coll_nr);

  size_t get_random_byte_amount(size_t min, size_t max);
  void get_filled_byte_array(bufferlist& bl, size_t size);

  void do_write_object(ObjectStore::Transaction *t,
      coll_t coll, hobject_t obj, C_StatState *stat);
  void do_setattr_object(ObjectStore::Transaction *t,
      coll_t coll, hobject_t obj, C_StatState *stat);
  void do_pgmeta_omap_set(ObjectStore::Transaction *t, spg_t pgid, coll_t coll,
      C_StatState *stat);
  void do_append_log(ObjectStore::Transaction *t, coll_entry_t *entry,
      C_StatState *stat);

  bool should_destroy_collection() {
    return ((m_destroy_coll_every_nr_runs > 0) &&
        ((int)m_nr_runs.read() >= m_destroy_coll_every_nr_runs));
  }
  void do_destroy_collection(ObjectStore::Transaction *t, coll_entry_t *entry,
      C_StatState *stat);
  coll_entry_t *do_create_collection(ObjectStore::Transaction *t,
      C_StatState *stat);

  void do_stats();

public:
  explicit WorkloadGenerator(vector<const char*> args);
  ~WorkloadGenerator() {
    m_store->umount();
  }

  class C_OnReadable: public TestObjectStoreState::C_OnFinished {
    WorkloadGenerator *wrkldgen_state;

  public:
    explicit C_OnReadable(WorkloadGenerator *state)
     :TestObjectStoreState::C_OnFinished(state), wrkldgen_state(state) { }

    void finish(int r)
    {
      TestObjectStoreState::C_OnFinished::finish(r);
      wrkldgen_state->m_nr_runs.inc();
    }
  };

  class C_OnDestroyed: public C_OnReadable {
    coll_entry_t *m_entry;

  public:
    C_OnDestroyed(WorkloadGenerator *state, coll_entry_t *entry) :
          C_OnReadable(state), m_entry(entry) {}

    void finish(int r) {
      C_OnReadable::finish(r);
      delete m_entry;
    }
  };

  class C_StatWrapper : public Context {
    C_StatState *stat_state;
    Context *ctx;

   public:
    C_StatWrapper(C_StatState *state, Context *context)
      : stat_state(state), ctx(context) { }

    void finish(int r) {
      ctx->complete(r);

      stat_state->wrkldgen->m_stats_lock.Lock();

      stat_state->wrkldgen->m_stats_total_written += stat_state->written_data;
      stat_state->wrkldgen->m_stats_finished_txs ++;
      stat_state->wrkldgen->m_stats_lock.Unlock();
    }
  };

  void run(void);
};

bool operator<(const WorkloadGenerator::coll_entry_t& l,
    const WorkloadGenerator::coll_entry_t& r) {
      return (l.m_id < r.m_id);
}

#endif /* WORKLOAD_GENERATOR_H_ */
