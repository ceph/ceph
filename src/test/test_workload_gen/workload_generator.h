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
#include <queue>
#include <set>

class WorkloadGenerator {
private:
  /* kept in upper case for consistency with coll_t's */
  static const coll_t META_COLL;
  static const coll_t TEMP_COLL;

  static const int max_in_flight = 50;

  static const int def_num_obj_per_coll = 6000;
  static const int def_num_colls = 30;
  static const int def_prob_destroy_coll = 30;
  static const int def_prob_create_coll = 10;

  static const size_t min_write_bytes = 1;
  static const size_t max_write_mb = 5;
  static const size_t max_write_bytes = (max_write_mb * 1024 * 1024);

  static const size_t min_xattr_obj_bytes = 2;
  static const size_t max_xattr_obj_bytes = 300;
  static const size_t min_xattr_coll_bytes = 4;
  static const size_t max_xattr_coll_bytes = 600;

  static const size_t log_append_bytes = 1024;

  /* probabilities for creating or destroying a collection */
  bool m_allow_coll_destruction;
  int m_prob_destroy_coll;
  int m_prob_create_coll;

  int m_num_colls;
  int m_num_obj_per_coll;

  boost::scoped_ptr<ObjectStore> m_store;

  int m_in_flight;
  vector<ObjectStore::Sequencer> m_osr;

  Mutex m_lock;
  Cond m_cond;

  set<coll_t> m_available_collections;
  set<coll_t> m_removed_collections;


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

  bool allow_collection_destruction();
  void do_destroy_collection(ObjectStore::Transaction *t);
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
      dout(0) << "Got one back!" << dendl;
      Mutex::Locker locker(m_state->m_lock);
      m_state->m_in_flight--;
      m_state->m_cond.Signal();
    }
  };

  void run(void);
  void print_results(void);
};

#endif /* WORKLOAD_GENERATOR_H_ */
