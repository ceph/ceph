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
#ifndef TEST_OBJECTSTORE_STATE_H_
#define TEST_OBJECTSTORE_STATE_H_

#include "os/ObjectStore.h"
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <map>
#include <vector>

typedef boost::mt11213b rngen_t;

class TestObjectStoreState {
public:
  struct coll_entry_t {
    int m_id;
    spg_t m_pgid;
    coll_t m_coll;
    ghobject_t m_meta_obj;
    ObjectStore::Sequencer m_osr;
    map<int, hobject_t*> m_objects;
    int m_next_object_id;

    coll_entry_t(int i, char *coll_buf, char *meta_obj_buf)
      : m_id(i),
	m_pgid(pg_t(i, 1), shard_id_t::NO_SHARD),
	m_coll(m_pgid),
	m_meta_obj(hobject_t(sobject_t(object_t(meta_obj_buf), CEPH_NOSNAP))),
      m_osr(coll_buf), m_next_object_id(0) {
    }
    ~coll_entry_t();

    hobject_t *touch_obj(int id);
    bool check_for_obj(int id);
    hobject_t *get_obj(int id);
    hobject_t *remove_obj(int id);
    hobject_t *get_obj_at(int pos, int *key = NULL);
    hobject_t *remove_obj_at(int pos, int *key = NULL);
    hobject_t *replace_obj(int id, hobject_t *obj);
    int get_random_obj_id(rngen_t& gen);

   private:
    hobject_t *get_obj(int id, bool remove);
    hobject_t *get_obj_at(int pos, bool remove, int *key = NULL);
  };

 protected:
  boost::shared_ptr<ObjectStore> m_store;
  map<int, coll_entry_t*> m_collections;
  vector<int> m_collections_ids;
  int m_next_coll_nr;
  int m_num_objs_per_coll;
  int m_num_objects;

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
  int get_next_pool_id() { return m_next_pool++; }

 private:
  static const int m_default_num_colls = 30;
  // The pool ID used for collection creation, ID 0 is preserve for other tests
  int m_next_pool;

 public:
  explicit TestObjectStoreState(ObjectStore *store) :
    m_next_coll_nr(0), m_num_objs_per_coll(10), m_num_objects(0),
    m_max_in_flight(0), m_finished_lock("Finished Lock"), m_next_pool(1) {
    m_in_flight.set(0);
    m_store.reset(store);
  }
  ~TestObjectStoreState() { 
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
    TestObjectStoreState *m_state;

   public:
    explicit C_OnFinished(TestObjectStoreState *state) : m_state(state) { }

    void finish(int r) {
      Mutex::Locker locker(m_state->m_finished_lock);
      m_state->dec_in_flight();
      m_state->m_finished_cond.Signal();

    }
  };
};

#endif /* TEST_OBJECTSTORE_STATE_H_ */
