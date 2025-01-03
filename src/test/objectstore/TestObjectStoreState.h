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

#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <map>
#include <vector>

#include "os/ObjectStore.h"
#include "common/Cond.h"

typedef boost::mt11213b rngen_t;

class TestObjectStoreState {
public:
  struct coll_entry_t {
    spg_t m_pgid;
    coll_t m_cid;
    ghobject_t m_meta_obj;
    ObjectStore::CollectionHandle m_ch;
    std::map<int, hobject_t*> m_objects;
    int m_next_object_id;

    coll_entry_t(spg_t pgid, ObjectStore::CollectionHandle& ch,
		 char *meta_obj_buf)
      : m_pgid(pgid),
	m_cid(m_pgid),
	m_meta_obj(hobject_t(sobject_t(object_t(meta_obj_buf), CEPH_NOSNAP))),
	m_ch(ch),
	m_next_object_id(0) {
      m_meta_obj.hobj.pool = m_pgid.pool();
      m_meta_obj.hobj.set_hash(m_pgid.ps());
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
  std::map<coll_t, coll_entry_t*> m_collections;
  std::vector<coll_t> m_collections_ids;
  int m_next_coll_nr;
  int m_num_objs_per_coll;
  int m_num_objects;

  int m_max_in_flight;
  std::atomic<int> m_in_flight = { 0 };
  ceph::mutex m_finished_lock = ceph::make_mutex("Finished Lock");
  ceph::condition_variable m_finished_cond;

  void rebuild_id_vec() {
    m_collections_ids.clear();
    m_collections_ids.reserve(m_collections.size());
    for (auto& i : m_collections) {
      m_collections_ids.push_back(i.first);
    }
  }

  void wait_for_ready() {
    std::unique_lock locker{m_finished_lock};
    m_finished_cond.wait(locker, [this] {
      return m_max_in_flight <= 0 || m_in_flight < m_max_in_flight;
    });
  }

  void wait_for_done() {
    std::unique_lock locker{m_finished_lock};
    m_finished_cond.wait(locker, [this] { return m_in_flight == 0; });
  }

  void set_max_in_flight(int max) {
    m_max_in_flight = max;
  }
  void set_num_objs_per_coll(int val) {
    m_num_objs_per_coll = val;
  }

  coll_entry_t *get_coll(coll_t cid, bool erase = false);
  coll_entry_t *get_coll_at(int pos, bool erase = false);
  int get_next_pool_id() { return m_next_pool++; }

 private:
  static const int m_default_num_colls = 30;
  // The pool ID used for collection creation, ID 0 is preserve for other tests
  int m_next_pool;

 public:
  explicit TestObjectStoreState(ObjectStore *store) :
    m_next_coll_nr(0), m_num_objs_per_coll(10), m_num_objects(0),
    m_max_in_flight(0), m_next_pool(2) {
    m_store.reset(store);
  }
  ~TestObjectStoreState() { 
    auto it = m_collections.begin();
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
    return ++m_in_flight;
  }

  int dec_in_flight() {
    return --m_in_flight;
  }

  coll_entry_t *coll_create(spg_t pgid, ObjectStore::CollectionHandle ch);

  class C_OnFinished: public Context {
   protected:
    TestObjectStoreState *m_state;

   public:
    explicit C_OnFinished(TestObjectStoreState *state) : m_state(state) { }

    void finish(int r) override {
      std::lock_guard locker{m_state->m_finished_lock};
      m_state->dec_in_flight();
      m_state->m_finished_cond.notify_all();

    }
  };
};

#endif /* TEST_OBJECTSTORE_STATE_H_ */
