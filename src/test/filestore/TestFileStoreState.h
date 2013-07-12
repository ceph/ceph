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
* Foundation. See file COPYING.
*/
#ifndef TEST_FILESTORE_STATE_H_
#define TEST_FILESTORE_STATE_H_

#include "os/FileStore.h"
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <map>
#include <vector>

class TestFileStoreState {
public:
  struct coll_entry_t {
    int m_id;
    coll_t m_coll;
    hobject_t m_meta_obj;
    ObjectStore::Sequencer m_osr;
    map<int, hobject_t*> m_objects;
    int m_next_object_id;

    coll_entry_t(int i, char *coll_buf, char *meta_obj_buf)
    : m_id(i), m_coll(coll_buf),
      m_meta_obj(sobject_t(object_t(meta_obj_buf), CEPH_NOSNAP)),
      m_osr(coll_buf), m_next_object_id(0) {
    }
    ~coll_entry_t();

    hobject_t *touch_obj(int id);
    hobject_t *get_obj(int id);
    hobject_t *remove_obj(int id);
    hobject_t *get_obj_at(int pos, int *key = NULL);
    hobject_t *remove_obj_at(int pos, int *key = NULL);
    hobject_t *replace_obj(int id, hobject_t *obj);

   private:
    hobject_t *get_obj(int id, bool remove);
    hobject_t *get_obj_at(int pos, bool remove, int *key = NULL);
  };

  /* kept in upper case for consistency with coll_t's */
  static const coll_t META_COLL;
  static const coll_t TEMP_COLL;

 protected:
  boost::shared_ptr<ObjectStore> m_store;
  map<int, coll_entry_t*> m_collections;
  vector<int> m_collections_ids;
  int m_next_coll_nr;
  int m_num_objs_per_coll;

  int m_max_in_flight;
  atomic_t m_in_flight;
  Mutex m_finished_lock;
  Cond m_finished_cond;

  void wait_for_ready() {
    Mutex::Locker locker(m_finished_lock);
    while ((m_max_in_flight > 0) && ((int)m_in_flight.read() >= m_max_in_flight))
      m_finished_cond.Wait(m_finished_lock);
  }

  void wait_for_done() {
    Mutex::Locker locker(m_finished_lock);
    while (m_in_flight.read())
      m_finished_cond.Wait(m_finished_lock);
  }

  void set_max_in_flight(int max) {
    m_max_in_flight = max;
  }
  void set_num_objs_per_coll(int val) {
    m_num_objs_per_coll = val;
  }

  coll_entry_t *get_coll(int key, bool erase = false);
  coll_entry_t *get_coll_at(int pos, bool erase = false);

 private:
  static const int m_default_num_colls = 30;

 public:
  TestFileStoreState(FileStore *store) :
    m_next_coll_nr(0), m_num_objs_per_coll(10),
    m_max_in_flight(0), m_finished_lock("Finished Lock") {
    m_in_flight.set(0);
    m_store.reset(store);
  }
  ~TestFileStoreState() { 
    map<int, coll_entry_t*>::iterator it = m_collections.begin();
    while (it != m_collections.end()) {
      if (it->second)
	delete it->second;
      m_collections.erase(it++);
    }
  }

  void init(int colls, int objs);
  void init() {
    init(m_default_num_colls, 0);
  }

  int inc_in_flight() {
    return ((int) m_in_flight.inc());
  }

  int dec_in_flight() {
    return ((int) m_in_flight.dec());
  }

  coll_entry_t *coll_create(int id);

  class C_OnFinished: public Context {
   protected:
    TestFileStoreState *m_state;
    ObjectStore::Transaction *m_tx;

   public:
    C_OnFinished(TestFileStoreState *state,
        ObjectStore::Transaction *t) : m_state(state), m_tx(t) { }

    void finish(int r) {
      Mutex::Locker locker(m_state->m_finished_lock);
      m_state->dec_in_flight();
      m_state->m_finished_cond.Signal();

      delete m_tx;
    }
  };
};

#endif /* TEST_FILESTORE_STATE_H_ */
