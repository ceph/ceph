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

#include "os/FileStore.h"
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <map>

#include "TestFileStoreState.h"

typedef boost::mt11213b rngen_t;

class WorkloadGenerator : public TestFileStoreState {
public:
  static const int max_in_flight = 50;

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

private:
  int m_num_ops;
  int m_destroy_coll_every_nr_runs;
  atomic_t m_nr_runs;

  int m_num_colls;

  rngen_t m_rng;

  map<coll_t, uint64_t> pg_log_size;

  void init_args(vector<const char*> args);

  int get_uniform_random_value(int min, int max);
  coll_entry_t *get_rnd_coll_entry(bool erase);
  hobject_t *get_rnd_obj(coll_entry_t *entry);
  int get_random_collection_nr();
  int get_random_object_nr(int coll_nr);

  size_t get_random_byte_amount(size_t min, size_t max);
  void get_filled_byte_array(bufferlist& bl, size_t size);

  void do_write_object(ObjectStore::Transaction *t,
      coll_t coll, hobject_t obj);
  void do_setattr_object(ObjectStore::Transaction *t,
      coll_t coll, hobject_t obj);
  void do_setattr_collection(ObjectStore::Transaction *t, coll_t coll);
//  void do_append_log(ObjectStore::Transaction *t, coll_t coll);
  void do_append_log(ObjectStore::Transaction *t, coll_entry_t *entry);

  bool should_destroy_collection() {
    return ((int)m_nr_runs.read() >= m_destroy_coll_every_nr_runs);
  }
  void do_destroy_collection(ObjectStore::Transaction *t, coll_entry_t *entry);
  coll_entry_t *do_create_collection(ObjectStore::Transaction *t);

public:
  WorkloadGenerator(vector<const char*> args);
  ~WorkloadGenerator() {
    m_store->umount();
  }

  class C_OnReadable: public TestFileStoreState::C_OnFinished {
    WorkloadGenerator *wrkldgen_state;

  public:
    C_OnReadable(WorkloadGenerator *state,
                                  ObjectStore::Transaction *t)
     :TestFileStoreState::C_OnFinished(state, t), wrkldgen_state(state) { }

    void finish(int r)
    {
      TestFileStoreState::C_OnFinished::finish(r);
      wrkldgen_state->m_nr_runs.inc();
    }
  };

  class C_OnDestroyed: public C_OnReadable {
    coll_entry_t *m_entry;

  public:
    C_OnDestroyed(WorkloadGenerator *state,
        ObjectStore::Transaction *t, coll_entry_t *entry) :
          C_OnReadable(state, t), m_entry(entry) {}

    void finish(int r) {
      C_OnReadable::finish(r);
      delete m_entry;
    }
  };

  void run(void);
  void print_results(void);
};

bool operator<(const WorkloadGenerator::coll_entry_t& l,
    const WorkloadGenerator::coll_entry_t& r) {
      return (l.m_id < r.m_id);
}

#endif /* WORKLOAD_GENERATOR_H_ */
