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
#include <set>

typedef boost::mt11213b rngen_t;

class WorkloadGenerator {
public:
  struct coll_entry_t {
    int id;
    coll_t coll;
    hobject_t meta_obj;
    ObjectStore::Sequencer osr;

    coll_entry_t(int i, char *coll_buf, char *meta_obj_buf)
    : id(i), coll(coll_buf),
      meta_obj(sobject_t(object_t(meta_obj_buf), CEPH_NOSNAP)),
      osr(coll_buf) {
    }
  };


  /* kept in upper case for consistency with coll_t's */
  static const coll_t META_COLL;
  static const coll_t TEMP_COLL;

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
  int m_destroy_coll_every_nr_runs;
  int m_num_colls;
  int m_num_obj_per_coll;

  boost::scoped_ptr<ObjectStore> m_store;

  int m_nr_runs;
  int m_in_flight;
//  vector<ObjectStore::Sequencer> m_osr;

  Mutex m_lock;
  Cond m_cond;

  set<coll_entry_t*> m_collections;
  int m_next_coll_nr;

  rngen_t m_rng;

  void wait_for_ready() {
    while (m_in_flight >= max_in_flight)
      m_cond.Wait(m_lock);
  }

  void wait_for_done() {
    Mutex::Locker locker(m_lock);
    while (m_in_flight)
      m_cond.Wait(m_lock);
  }

  void init_args(vector<const char*> args);
  void init();

  int get_uniform_random_value(int min, int max);
  coll_entry_t *get_rnd_coll_entry(bool erase);
  int get_random_collection_nr();
  int get_random_object_nr(int coll_nr);

  coll_t get_collection_by_nr(int nr);
  hobject_t get_object_by_nr(int nr);
  hobject_t get_coll_meta_object(coll_t coll);

  size_t get_random_byte_amount(size_t min, size_t max);
  void get_filled_byte_array(bufferlist& bl, size_t size);

  void do_write_object(ObjectStore::Transaction *t,
      coll_t coll, hobject_t obj);
  void do_setattr_object(ObjectStore::Transaction *t,
      coll_t coll, hobject_t obj);
  void do_setattr_collection(ObjectStore::Transaction *t, coll_t coll);
  void do_append_log(ObjectStore::Transaction *t, coll_t coll);

  bool should_destroy_collection() {
    return (m_nr_runs >= m_destroy_coll_every_nr_runs);
  }
  void do_destroy_collection(ObjectStore::Transaction *t, coll_entry_t *entry);
  void do_create_collection(ObjectStore::Transaction *t);

public:
  WorkloadGenerator(vector<const char*> args);
  ~WorkloadGenerator() {
    m_store->umount();
  }

  class C_WorkloadGeneratorOnReadable: public Context {
    WorkloadGenerator *m_state;
    ObjectStore::Transaction *m_tx;

  public:
    C_WorkloadGeneratorOnReadable(WorkloadGenerator *state,
        ObjectStore::Transaction *t) :
      m_state(state), m_tx(t) {
    }

    void finish(int r) {
//      dout(0) << "Got one back!" << dendl;
      Mutex::Locker locker(m_state->m_lock);
      m_state->m_in_flight--;
      m_state->m_nr_runs++;
      m_state->m_cond.Signal();

      delete m_tx;
    }
  };

  class C_WorkloadGeneratorOnDestroyed: public C_WorkloadGeneratorOnReadable {
//    WorkloadGenerator *m_state;
//    ObjectStore::Transaction *m_tx;
    coll_entry_t *m_entry;

  public:
    C_WorkloadGeneratorOnDestroyed(WorkloadGenerator *state,
        ObjectStore::Transaction *t, coll_entry_t *entry) :
          C_WorkloadGeneratorOnReadable(state, t), m_entry(entry) {}

    void finish(int r) {
      C_WorkloadGeneratorOnReadable::finish(r);
      //dout(0) << "Destroyed collection " << m_entry->coll.to_str() << dendl;
      delete m_entry;
    }
  };

  void run(void);
  void print_results(void);
};

bool operator<(const WorkloadGenerator::coll_entry_t& l,
    const WorkloadGenerator::coll_entry_t& r) {
      return (l.id < r.id);
}

#endif /* WORKLOAD_GENERATOR_H_ */
