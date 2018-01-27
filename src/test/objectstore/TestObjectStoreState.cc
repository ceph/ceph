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
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <stdlib.h>
#include <signal.h>
#include "os/ObjectStore.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/debug.h"
#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include "TestObjectStoreState.h"
#include "include/assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "ceph_test_objectstore_state "

void TestObjectStoreState::init(int colls, int objs)
{
  dout(5) << "init " << colls << " colls " << objs << " objs" << dendl;

  ObjectStore::Transaction t;
  auto meta_ch = m_store->create_new_collection(coll_t::meta());
  t.create_collection(coll_t::meta(), 0);
  m_store->queue_transaction(meta_ch, std::move(t));

  wait_for_ready();

  int baseid = 0;
  for (int i = 0; i < colls; i++) {
    spg_t pgid(pg_t(i, 1), shard_id_t::NO_SHARD);
    coll_t cid(pgid);
    auto ch = m_store->create_new_collection(cid);
    coll_entry_t *entry = coll_create(pgid, ch);
    dout(5) << "init create collection " << entry->m_cid
        << " meta " << entry->m_meta_obj << dendl;

    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->create_collection(entry->m_cid, 32);
    bufferlist hint;
    uint32_t pg_num = colls;
    uint64_t num_objs = uint64_t(objs / colls);
    encode(pg_num, hint);
    encode(num_objs, hint);
    t->collection_hint(entry->m_cid, ObjectStore::Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS, hint);
    dout(5) << "give collection hint, number of objects per collection: " << num_objs << dendl;
    t->touch(cid, entry->m_meta_obj);

    for (int i = 0; i < objs; i++) {
      hobject_t *obj = entry->touch_obj(i + baseid);
      t->touch(entry->m_cid, ghobject_t(*obj));
      ceph_assert(i + baseid == m_num_objects);
      m_num_objects++;
    }
    baseid += objs;

    t->register_on_commit(new C_OnFinished(this));
    m_store->queue_transaction(entry->m_ch, std::move(*t), nullptr);

    delete t;
    inc_in_flight();

    m_collections.insert(make_pair(cid, entry));
    rebuild_id_vec();
    m_next_coll_nr++;
  }
  dout(5) << "init has " << m_in_flight.load() << "in-flight transactions" << dendl;
  wait_for_done();
  dout(5) << "init finished" << dendl;
}

TestObjectStoreState::coll_entry_t *TestObjectStoreState::coll_create(
  spg_t pgid, ObjectStore::CollectionHandle ch)
{
  char meta_buf[100];
  memset(meta_buf, 0, 100);
  snprintf(meta_buf, 100, "pglog_0_head");
  return (new coll_entry_t(pgid, ch, meta_buf));
}

TestObjectStoreState::coll_entry_t*
TestObjectStoreState::get_coll(coll_t cid, bool erase)
{
  dout(5) << "get_coll id " << cid << dendl;

  coll_entry_t *entry = NULL;
  auto it = m_collections.find(cid);
  if (it != m_collections.end()) {
    entry = it->second;
    if (erase) {
      m_collections.erase(it);
      rebuild_id_vec();
    }
  }

  dout(5) << "get_coll id " << cid;
  if (!entry)
    *_dout << " non-existent";
  else
    *_dout << " name " << entry->m_cid;
  *_dout << dendl;
  return entry;
}

TestObjectStoreState::coll_entry_t*
TestObjectStoreState::get_coll_at(int pos, bool erase)
{
  dout(5) << "get_coll_at pos " << pos << dendl;

  if (m_collections.empty())
    return NULL;

  assert((size_t) pos < m_collections_ids.size());

  coll_t cid = m_collections_ids[pos];
  coll_entry_t *entry = m_collections[cid];

  if (entry == NULL) {
    dout(5) << "get_coll_at pos " << pos << " non-existent" << dendl;
    return NULL;
  }

  if (erase) {
    m_collections.erase(cid);
    rebuild_id_vec();
  }

  dout(5) << "get_coll_at pos " << pos << ": "
      << entry->m_cid << "(removed: " << erase << ")" << dendl;

  return entry;
}

TestObjectStoreState::coll_entry_t::~coll_entry_t()
{
  if (m_objects.size() > 0) {
    map<int, hobject_t*>::iterator it = m_objects.begin();
    for (; it != m_objects.end(); ++it) {
      hobject_t *obj = it->second;
      if (obj) {
        delete obj;
      }
    }
    m_objects.clear();
  }
}

bool TestObjectStoreState::coll_entry_t::check_for_obj(int id)
{
  if (m_objects.count(id))
    return true;
  return false;
}

hobject_t *TestObjectStoreState::coll_entry_t::touch_obj(int id)
{
  map<int, hobject_t*>::iterator it = m_objects.find(id);
  if (it != m_objects.end()) {
    dout(5) << "touch_obj coll id " << m_cid
        << " name " << it->second->oid.name << dendl;
    return it->second;
  }

  char buf[100];
  memset(buf, 0, 100);
  snprintf(buf, 100, "obj%d", id);

  hobject_t *obj = new hobject_t(sobject_t(object_t(buf), CEPH_NOSNAP));
  obj->set_hash(m_pgid.ps());
  obj->pool = m_pgid.pool();
  m_objects.insert(make_pair(id, obj));

  dout(5) << "touch_obj coll id " << m_cid << " name " << buf << dendl;
  return obj;
}

hobject_t *TestObjectStoreState::coll_entry_t::get_obj(int id)
{
  return get_obj(id, false);
}

/**
 * remove_obj - Removes object without freeing it.
 * @param id Object's id in the map.
 * @return The object or NULL in case of error.
 */
hobject_t *TestObjectStoreState::coll_entry_t::remove_obj(int id)
{
  return get_obj(id, true);
}

hobject_t *TestObjectStoreState::coll_entry_t::get_obj(int id, bool remove)
{
  map<int, hobject_t*>::iterator it = m_objects.find(id);
  if (it == m_objects.end()) {
    dout(5) << "get_obj coll " << m_cid
        << " obj #" << id << " non-existent" << dendl;
    return NULL;
  }

  hobject_t *obj = it->second;
  if (remove)
    m_objects.erase(it);

  dout(5) << "get_obj coll " << m_cid << " id " << id
      << ": " << obj->oid.name << "(removed: " << remove << ")" << dendl;

  return obj;
}

hobject_t *TestObjectStoreState::coll_entry_t::get_obj_at(int pos, int *key)
{
  return get_obj_at(pos, false, key);
}

/**
 * remove_obj_at - Removes object without freeing it.
 * @param pos The map's position in which the object lies.
 * @return The object or NULL in case of error.
 */
hobject_t *TestObjectStoreState::coll_entry_t::remove_obj_at(int pos, int *key)
{
  return get_obj_at(pos, true, key);
}

hobject_t *TestObjectStoreState::coll_entry_t::get_obj_at(int pos,
    bool remove, int *key)
{
  if (m_objects.empty()) {
    dout(5) << "get_obj_at coll " << m_cid << " pos " << pos
        << " in an empty collection" << dendl;
    return NULL;
  }

  hobject_t *ret = NULL;
  map<int, hobject_t*>::iterator it = m_objects.begin();
  for (int i = 0; it != m_objects.end(); ++it, i++) {
    if (i == pos) {
      ret = it->second;
      break;
    }
  }

  if (ret == NULL) {
    dout(5) << "get_obj_at coll " << m_cid << " pos " << pos
        << " non-existent" << dendl;
    return NULL;
  }

  if (key != NULL)
    *key = it->first;

  if (remove)
    m_objects.erase(it);

  dout(5) << "get_obj_at coll id " << m_cid << " pos " << pos
      << ": " << ret->oid.name << "(removed: " << remove << ")" << dendl;

  return ret;
}

hobject_t*
TestObjectStoreState::coll_entry_t::replace_obj(int id, hobject_t *obj) {
  hobject_t *old_obj = remove_obj(id);
  m_objects.insert(make_pair(id, obj));
  return old_obj;
}

int TestObjectStoreState::coll_entry_t::get_random_obj_id(rngen_t& gen)
{
  ceph_assert(!m_objects.empty());

  boost::uniform_int<> orig_obj_rng(0, m_objects.size()-1);
  int pos = orig_obj_rng(gen);
  map<int, hobject_t*>::iterator it = m_objects.begin();
  for (int i = 0; it != m_objects.end(); ++it, i++) {
    if (i == pos) {
      return it->first;
    }
  }
  ceph_assert(0 == "INTERNAL ERROR");
}
