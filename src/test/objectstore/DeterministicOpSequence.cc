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
#include <fstream>
#include <time.h>
#include <stdlib.h>
#include <signal.h>
#include <sstream>
#include "os/ObjectStore.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/debug.h"
#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>

#include "DeterministicOpSequence.h"
#include "common/config.h"
#include "include/assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "deterministic_seq "

DeterministicOpSequence::DeterministicOpSequence(ObjectStore *store,
						 std::string status)
  : TestObjectStoreState(store),
    txn(0)
{
  if (!status.empty())
    m_status.open(status.c_str());
}

DeterministicOpSequence::~DeterministicOpSequence()
{
  // TODO Auto-generated destructor stub
}

bool DeterministicOpSequence::run_one_op(int op, rngen_t& gen)
{
  bool ok = false;
  switch (op) {
  case DSOP_TOUCH:
    ok = do_touch(gen);
    break;
  case DSOP_WRITE:
    ok = do_write(gen);
    break;
  case DSOP_CLONE:
    ok = do_clone(gen);
    break;
  case DSOP_CLONE_RANGE:
    ok = do_clone_range(gen);
    break;
  case DSOP_OBJ_REMOVE:
    ok = do_remove(gen);
    break;
  case DSOP_COLL_MOVE:
    ok = do_coll_move(gen);
    break;
  case DSOP_SET_ATTRS:
    ok = do_set_attrs(gen);
    break;
  case DSOP_COLL_CREATE:
    ok = do_coll_create(gen);
    break;

  default:
    cout << "bad op " << op << std::endl;
    assert(0 == "bad op");
  }
  return ok;
}

void DeterministicOpSequence::generate(int seed, int num_txs)
{
  std::ostringstream ss;
  ss << "generate run " << num_txs << " --seed " << seed;

  if (m_status.is_open()) {
    m_status << ss.str() << std::endl;
    m_status.flush();
  }

  dout(0) << ss.str() << dendl;

  rngen_t gen(seed);
  boost::uniform_int<> op_rng(DSOP_FIRST, DSOP_LAST);

  for (txn = 1; txn <= num_txs; ) {
    int op = op_rng(gen);
    _print_status(txn, op);
    dout(0) << "generate seq " << txn << " op " << op << dendl;
    if (run_one_op(op, gen))
      txn++;
  }
}

void DeterministicOpSequence::_print_status(int seq, int op)
{
  if (!m_status.is_open())
    return;
  m_status << seq << " " << op << std::endl;
  m_status.flush();
}

int DeterministicOpSequence::_gen_coll_id(rngen_t& gen)
{
  boost::uniform_int<> coll_rng(0, m_collections_ids.size()-1);
  return coll_rng(gen);
}

int DeterministicOpSequence::_gen_obj_id(rngen_t& gen)
{
  boost::uniform_int<> obj_rng(0, m_num_objects - 1);
  return obj_rng(gen);
}

void DeterministicOpSequence::note_txn(coll_entry_t *entry,
				       ObjectStore::Transaction *t)
{
  bufferlist bl;
  encode(txn, bl);
  ghobject_t oid = get_txn_object(entry->m_cid);
  t->truncate(entry->m_cid, oid, 0);
  t->write(entry->m_cid, oid, 0, bl.length(), bl);
  dout(10) << __func__ << " " << txn << dendl;
}

bool DeterministicOpSequence::do_touch(rngen_t& gen)
{
  int coll_id = _gen_coll_id(gen);
  int obj_id = _gen_obj_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  ceph_assert(entry != NULL);

  // Don't care about other collections if already exists
  if (!entry->check_for_obj(obj_id)) {
    bool other_found = false;
    auto it = m_collections.begin();
    for (; it != m_collections.end(); ++it) {
      if (it->second->check_for_obj(obj_id)) {
        ceph_assert(it->first != entry->m_cid);
        other_found = true;
      }
    }
    if (other_found) {
      dout(0) << "do_touch new object in collection and exists in another" << dendl;
      return false;
    }
  }
  hobject_t *obj = entry->touch_obj(obj_id);

  dout(0) << "do_touch " << entry->m_cid << "/" << obj << dendl;

  _do_touch(entry, *obj);
  return true;
}

bool DeterministicOpSequence::do_remove(rngen_t& gen)
{
  int coll_id = _gen_coll_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  ceph_assert(entry != NULL);

  if (entry->m_objects.size() == 0) {
    dout(0) << "do_remove no objects in collection" << dendl;
    return false;
  }
  int obj_id = entry->get_random_obj_id(gen);
  hobject_t *obj = entry->touch_obj(obj_id);
  ceph_assert(obj);

  dout(0) << "do_remove " << entry->m_cid << "/" << obj << dendl;

  _do_remove(entry, *obj);
  hobject_t *rmobj = entry->remove_obj(obj_id);
  ceph_assert(rmobj);
  delete rmobj;
  return true;
}

static void _gen_random(rngen_t& gen,
			size_t size, bufferlist& bl) {

  static const char alphanum[] = "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

  boost::uniform_int<> char_rng(0, sizeof(alphanum));
  bufferptr bp(size);
  for (unsigned int i = 0; i < size - 1; i++) {
    bp[i] = alphanum[char_rng(gen)];
  }
  bp[size - 1] = '\0';
  bl.append(bp);
}

static void gen_attrs(rngen_t &gen,
		      map<string, bufferlist> *out) {
  boost::uniform_int<> num_rng(10, 30);
  boost::uniform_int<> key_size_rng(5, 10);
  boost::uniform_int<> val_size_rng(100, 1000);
  size_t num_attrs = static_cast<size_t>(num_rng(gen));
  for (size_t i = 0; i < num_attrs; ++i) {
    size_t key_size = static_cast<size_t>(num_rng(gen));
    size_t val_size = static_cast<size_t>(num_rng(gen));
    bufferlist keybl;
    _gen_random(gen, key_size, keybl);
    string key(keybl.c_str(), keybl.length());
    _gen_random(gen, val_size, (*out)[key]);
  }
}

bool DeterministicOpSequence::do_set_attrs(rngen_t& gen) 
{
  int coll_id = _gen_coll_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  ceph_assert(entry != NULL);

  if (entry->m_objects.size() == 0) {
    dout(0) << "do_set_attrs no objects in collection" << dendl;
    return false;
  }
  int obj_id = entry->get_random_obj_id(gen);
  hobject_t *obj = entry->touch_obj(obj_id);
  ceph_assert(obj);

  map<string, bufferlist> out;
  gen_attrs(gen, &out);

  dout(0) << "do_set_attrs " << out.size() << " entries" << dendl;
  _do_set_attrs(entry, *obj, out);
  return true;
}

bool DeterministicOpSequence::do_write(rngen_t& gen)
{
  int coll_id = _gen_coll_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  ceph_assert(entry != NULL);

  if (entry->m_objects.size() == 0) {
    dout(0) << "do_write no objects in collection" << dendl;
    return false;
  }
  int obj_id = entry->get_random_obj_id(gen);
  hobject_t *obj = entry->touch_obj(obj_id);
  ceph_assert(obj);

  boost::uniform_int<> size_rng(100, (2 << 19));
  size_t size = (size_t) size_rng(gen);
  bufferlist bl;
  _gen_random(gen, size, bl);

  dout(0) << "do_write " << entry->m_cid << "/" << obj
	  << " 0~" << size << dendl;

  _do_write(entry, *obj, 0, bl.length(), bl);
  return true;
}

bool DeterministicOpSequence::_prepare_clone(
  rngen_t& gen,
  coll_entry_t **entry_ret,
  int *orig_obj_id,
  hobject_t *orig_obj_ret,
  int *new_obj_id,
  hobject_t *new_obj_ret)
{
  int coll_id = _gen_coll_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  ceph_assert(entry != NULL);

  if (entry->m_objects.size() < 2) {
    dout(0) << "_prepare_clone coll " << entry->m_cid
	    << " doesn't have 2 or more objects" << dendl;
    return false;
  }

  *orig_obj_id = entry->get_random_obj_id(gen);
  hobject_t *orig_obj = entry->touch_obj(*orig_obj_id);
  ceph_assert(orig_obj);

  do {
    *new_obj_id = entry->get_random_obj_id(gen);
  } while (*new_obj_id == *orig_obj_id);
  hobject_t *new_obj = entry->touch_obj(*new_obj_id);
  ceph_assert(new_obj);

  *entry_ret = entry;
  *orig_obj_ret = *orig_obj;
  *new_obj_ret = *new_obj;
  return true;
}

bool DeterministicOpSequence::do_clone(rngen_t& gen)
{
  coll_entry_t *entry;
  int orig_id, new_id;
  hobject_t orig_obj, new_obj;
  if (!_prepare_clone(gen, &entry, &orig_id, &orig_obj, &new_id, &new_obj)) {
    return false;
  }

  dout(0) << "do_clone " << entry->m_cid << "/" << orig_obj
      << " => " << entry->m_cid << "/" << new_obj << dendl;

  _do_clone(entry, orig_obj, new_obj);
  return true;
}

bool DeterministicOpSequence::do_clone_range(rngen_t& gen)
{
  coll_entry_t *entry;
  int orig_id, new_id;
  hobject_t orig_obj, new_obj;
  if (!_prepare_clone(gen, &entry, &orig_id, &orig_obj, &new_id, &new_obj)) {
    return false;
  }

  /* Whenever we have to make a clone_range() operation, just write to the
   * object first, so we know we have something to clone in the said range.
   * This may not be the best solution ever, but currently we're not keeping
   * track of the written-to objects, and until we know for sure we really
   * need to, let's just focus on the task at hand.
   */

  boost::uniform_int<> write_size_rng(100, (2 << 19));
  size_t size = (size_t) write_size_rng(gen);
  bufferlist bl;
  _gen_random(gen, size, bl);

  boost::uniform_int<> clone_len(1, bl.length());
  size = (size_t) clone_len(gen);

  dout(0) << "do_clone_range " << entry->m_cid << "/" << orig_obj
      << " (0~" << size << ")"
      << " => " << entry->m_cid << "/" << new_obj
      << " (0)" << dendl;
  _do_write_and_clone_range(entry, orig_obj, new_obj, 0, size, 0, bl);
  return true;
}

bool DeterministicOpSequence::do_coll_move(rngen_t& gen)
{
  coll_entry_t *entry;
  int orig_id, new_id;
  hobject_t orig_obj, new_obj;
  if (!_prepare_clone(gen, &entry, &orig_id, &orig_obj, &new_id, &new_obj)) {
    return false;
  }

  dout(0) << "do_coll_move " << entry->m_cid << "/" << orig_obj
        << " => " << entry->m_cid << "/" << new_obj << dendl;
  entry->remove_obj(orig_id);

  _do_coll_move(entry, orig_obj, new_obj);

  return true;
}

bool DeterministicOpSequence::do_coll_create(rngen_t& gen)
{
  int i = m_collections.size();
  spg_t pgid(pg_t(i, 1), shard_id_t::NO_SHARD);
  coll_t cid(pgid);
  auto ch = m_store->create_new_collection(cid);
  coll_entry_t *entry = coll_create(pgid, ch);
  m_collections.insert(make_pair(cid, entry));
  rebuild_id_vec();

  _do_coll_create(entry, 10, 10);
  
  return true;
}

void DeterministicOpSequence::_do_coll_create(coll_entry_t *entry, uint32_t pg_num, uint64_t num_objs)
{
  ObjectStore::Transaction t;
  t.create_collection(entry->m_cid, 32);
  note_txn(entry, &t);
  bufferlist hint;
  encode(pg_num, hint);
  encode(num_objs, hint);
  t.collection_hint(entry->m_cid, ObjectStore::Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS, hint);
  dout(0) << "Give collection: " << entry->m_cid
	  << " a hint, pg_num is: " << pg_num << ", num_objs is: "
	  << num_objs << dendl;

  m_store->queue_transaction(entry->m_ch, std::move(t));
}

void DeterministicOpSequence::_do_touch(coll_entry_t *entry, hobject_t& obj)
{
  ObjectStore::Transaction t;
  note_txn(entry, &t);
  t.touch(entry->m_cid, ghobject_t(obj));
  m_store->queue_transaction(entry->m_ch, std::move(t));
}

void DeterministicOpSequence::_do_remove(coll_entry_t *entry, hobject_t& obj)
{
  ObjectStore::Transaction t;
  note_txn(entry, &t);
  t.remove(entry->m_cid, ghobject_t(obj));
  m_store->queue_transaction(entry->m_ch, std::move(t));
}

void DeterministicOpSequence::_do_set_attrs(coll_entry_t *entry,
					    hobject_t &obj,
					    const map<string, bufferlist> &attrs)
{
  ObjectStore::Transaction t;
  note_txn(entry, &t);
  t.omap_setkeys(entry->m_cid, ghobject_t(obj), attrs);
  m_store->queue_transaction(entry->m_ch, std::move(t));
}

void DeterministicOpSequence::_do_write(coll_entry_t *entry, hobject_t& obj,
					uint64_t off, uint64_t len, const bufferlist& data)
{
  ObjectStore::Transaction t;
  note_txn(entry, &t);
  t.write(entry->m_cid, ghobject_t(obj), off, len, data);
  m_store->queue_transaction(entry->m_ch, std::move(t));
}

void DeterministicOpSequence::_do_clone(coll_entry_t *entry, hobject_t& orig_obj,
					hobject_t& new_obj)
{
  ObjectStore::Transaction t;
  note_txn(entry, &t);
  t.clone(entry->m_cid, ghobject_t(orig_obj), ghobject_t(new_obj));
  m_store->queue_transaction(entry->m_ch, std::move(t));
}

void DeterministicOpSequence::_do_clone_range(coll_entry_t *entry,
					      hobject_t& orig_obj, hobject_t& new_obj, uint64_t srcoff,
					      uint64_t srclen, uint64_t dstoff)
{
  ObjectStore::Transaction t;
  note_txn(entry, &t);
  t.clone_range(entry->m_cid, ghobject_t(orig_obj), ghobject_t(new_obj),
		srcoff, srclen, dstoff);
  m_store->queue_transaction(entry->m_ch, std::move(t));
}

void DeterministicOpSequence::_do_write_and_clone_range(coll_entry_t *entry,
                                                        hobject_t& orig_obj,
                                                        hobject_t& new_obj,
                                                        uint64_t srcoff,
                                                        uint64_t srclen,
                                                        uint64_t dstoff,
                                                        bufferlist& bl)
{
  ObjectStore::Transaction t;
  note_txn(entry, &t);
  t.write(entry->m_cid, ghobject_t(orig_obj), srcoff, bl.length(), bl);
  t.clone_range(entry->m_cid, ghobject_t(orig_obj), ghobject_t(new_obj),
		srcoff, srclen, dstoff);
  m_store->queue_transaction(entry->m_ch, std::move(t));
}

void DeterministicOpSequence::_do_coll_move(coll_entry_t *entry,
					    hobject_t& orig_obj,
					    hobject_t& new_obj)
{
  ObjectStore::Transaction t;
  note_txn(entry, &t);
  t.remove(entry->m_cid, ghobject_t(new_obj));
  t.collection_move_rename(entry->m_cid, ghobject_t(orig_obj),
			   entry->m_cid, ghobject_t(new_obj));
  m_store->queue_transaction(entry->m_ch, std::move(t));
}

