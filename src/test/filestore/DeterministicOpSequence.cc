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
#include "os/FileStore.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/debug.h"
#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>

#include "DeterministicOpSequence.h"

#include "common/config.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "deterministic_seq "

DeterministicOpSequence::DeterministicOpSequence(FileStore *store,
						 std::string status)
  : TestFileStoreState(store),
    txn(0),
    m_osr("OSR")
{
  txn_coll = coll_t("meta");
  txn_object = hobject_t(sobject_t("txn", CEPH_NOSNAP));

  if (!status.empty())
    m_status.open(status.c_str());
}

DeterministicOpSequence::~DeterministicOpSequence()
{
  // TODO Auto-generated destructor stub
}

void DeterministicOpSequence::run_one_op(int op, rngen_t& gen)
{
  switch (op) {
  case DSOP_TOUCH:
    do_touch(gen);
    break;
  case DSOP_WRITE:
    do_write(gen);
    break;
  case DSOP_CLONE:
    do_clone(gen);
    break;
  case DSOP_CLONE_RANGE:
    do_clone_range(gen);
    break;
  case DSOP_OBJ_REMOVE:
    do_remove(gen);
    break;
  case DSOP_COLL_MOVE:
    do_coll_move(gen);
    break;
  case DSOP_COLL_ADD:
    do_coll_add(gen);
    break;
  case DSOP_COLL_RENAME:
    //do_coll_rename(gen);
    break;
  case DSOP_SET_ATTRS:
    do_set_attrs(gen);
    break;
  default:
    assert(0 == "bad op");
  }
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

  for (txn = 1; txn <= num_txs; txn++) {
    int op = op_rng(gen);
    _print_status(txn, op);
    dout(0) << "generate seq " << txn << " op " << op << dendl;
    run_one_op(op, gen);
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
  boost::uniform_int<> obj_rng(0, m_num_objs_per_coll-1);
  return obj_rng(gen);
}

void DeterministicOpSequence::note_txn(ObjectStore::Transaction *t)
{
  bufferlist bl;
  ::encode(txn, bl);
  t->truncate(txn_coll, txn_object, 0);
  t->write(txn_coll, txn_object, 0, bl.length(), bl);
  dout(10) << __func__ << " " << txn << dendl;
}

void DeterministicOpSequence::do_touch(rngen_t& gen)
{
  int coll_id = _gen_coll_id(gen);
  int obj_id = _gen_obj_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  ceph_assert(entry != NULL);
  hobject_t *obj = entry->touch_obj(obj_id);

  dout(0) << "do_touch " << entry->m_coll.to_str() << "/" << obj->oid.name << dendl;

  _do_touch(entry->m_coll, *obj);
}

void DeterministicOpSequence::do_remove(rngen_t& gen)
{
  int coll_id = _gen_coll_id(gen);
  int obj_id = _gen_obj_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  ceph_assert(entry != NULL);
  hobject_t *obj = entry->touch_obj(obj_id);

  // ENOENT ok here.

  dout(0) << "do_remove " << entry->m_coll.to_str() << "/" << obj->oid.name << dendl;

  _do_touch(entry->m_coll, *obj);
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

void DeterministicOpSequence::do_set_attrs(rngen_t& gen) {
  int coll_id = _gen_coll_id(gen);
  int obj_id = _gen_obj_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  if (!entry) {
    dout(0) << "do_set_attrs coll id " << coll_id << " non-existent" << dendl;
    return;
  }

  hobject_t *obj = entry->get_obj(obj_id);
  if (!obj) {
    dout(0) << "do_set_attrs " << entry->m_coll.to_str()
	    << " no object #" << obj_id << dendl;
    return;
  }

  map<string, bufferlist> out;
  gen_attrs(gen, &out);

  dout(0) << "do_set_attrs " << out.size() << " entries" << dendl;
  return _do_set_attrs(entry->m_coll, *obj, out);
}

void DeterministicOpSequence::do_write(rngen_t& gen)
{
  int coll_id = _gen_coll_id(gen);
  int obj_id = _gen_obj_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  if (!entry) {
    dout(0) << "do_write coll id " << coll_id << " non-existent" << dendl;
    return;
  }
  hobject_t *obj = entry->touch_obj(obj_id);
  if (!obj) {
    dout(0) << "do_write " << entry->m_coll.to_str()
    << " no object #" << obj_id << dendl;
    return;
  }

  boost::uniform_int<> size_rng(100, (2 << 19));
  size_t size = (size_t) size_rng(gen);
  bufferlist bl;
  _gen_random(gen, size, bl);

  dout(0) << "do_write " << entry->m_coll.to_str() << "/" << obj->oid.name
	  << " 0~" << size << dendl;

  _do_write(entry->m_coll, *obj, 0, bl.length(), bl);
}

bool DeterministicOpSequence::_prepare_clone(rngen_t& gen,
					     coll_t& coll_ret, hobject_t& orig_obj_ret, hobject_t& new_obj_ret)
{
  int coll_id = _gen_coll_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  if (!entry) {
    dout(0) << "_prepare_clone coll id " << coll_id << " non-existent" << dendl;
    return false;
  }

  if (entry->m_objects.size() == 0) {
    dout(0) << "_prepare_clone coll " << entry->m_coll.to_str()
	    << " has no objects to clone" << dendl;
    return false;
  }

  boost::uniform_int<> orig_obj_rng(0, entry->m_objects.size()-1);
  int orig_obj_pos = orig_obj_rng(gen);
  int orig_obj_id = -1;
  hobject_t *orig_obj = entry->get_obj_at(orig_obj_pos, &orig_obj_id);

  int id;
  do {
    id = _gen_obj_id(gen);
  } while (id == orig_obj_id);
  hobject_t *new_obj = entry->touch_obj(id);

  coll_ret = entry->m_coll;
  orig_obj_ret = *orig_obj;
  new_obj_ret = *new_obj;

  return true;
}

void DeterministicOpSequence::do_clone(rngen_t& gen)
{
  coll_t coll;
  hobject_t orig_obj, new_obj;
  if (!_prepare_clone(gen, coll, orig_obj, new_obj)) {
    return;
  }

  dout(0) << "do_clone " << coll.to_str() << "/" << orig_obj.oid.name
      << " => " << coll.to_str() << "/" << new_obj.oid.name << dendl;

  _do_clone(coll, orig_obj, new_obj);
}

void DeterministicOpSequence::do_clone_range(rngen_t& gen)
{
  coll_t coll;
  hobject_t orig_obj, new_obj;
  if (!_prepare_clone(gen, coll, orig_obj, new_obj)) {
    return;
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

  dout(0) << "do_clone_range " << coll.to_str() << "/" << orig_obj.oid.name
      << " (0~" << size << ")"
      << " => " << coll.to_str() << "/" << new_obj.oid.name
      << " (0)" << dendl;
  _do_write_and_clone_range(coll, orig_obj, new_obj, 0, size, 0, bl);
}

bool DeterministicOpSequence::_prepare_colls(rngen_t& gen,
					     coll_entry_t* &orig_coll, coll_entry_t* &new_coll)
{
  int orig_coll_id = _gen_coll_id(gen);
  int new_coll_id = orig_coll_id;
  do {
    new_coll_id = _gen_coll_id(gen);
  } while (new_coll_id == orig_coll_id);

  dout(0) << "_prepare_colls from coll id " << orig_coll_id
      << " to coll id " << new_coll_id << dendl;

  orig_coll = get_coll_at(orig_coll_id);
  new_coll = get_coll_at(new_coll_id);

  if (!orig_coll || !new_coll) {
    dout(0) << "_prepare_colls " << " no such collection "
        << "(orig: " << orig_coll << " id " << orig_coll_id
        << " ; new: " << new_coll << " id" << new_coll_id << ")" << dendl;
    return false;
  }

  if (!orig_coll->m_objects.size()) {
    dout(0) << "_prepare_colls coll " << orig_coll->m_coll.to_str()
        << " has no objects to use" << dendl;
    return false;
  }

  return true;
}


void DeterministicOpSequence::do_coll_move(rngen_t& gen)
{
  coll_entry_t *orig_coll = NULL, *new_coll = NULL;
  if (!_prepare_colls(gen, orig_coll, new_coll))
    return;

  if (!orig_coll || !new_coll)
    return;

  boost::uniform_int<> obj_rng(0, orig_coll->m_objects.size()-1);
  int obj_pos = obj_rng(gen);
  int obj_key = -1;
  hobject_t *obj = orig_coll->remove_obj_at(obj_pos, &obj_key);

  hobject_t *new_coll_old_obj = new_coll->replace_obj(obj_key, obj);
  dout(0) << "do_coll_move " << orig_coll->m_coll.to_str() << "/" << obj->oid.name
	  << " => " << new_coll->m_coll.to_str() << "/" << obj->oid.name << dendl;
  if (new_coll_old_obj != NULL) {
    dout(0) << "do_coll_move replacing obj " << new_coll->m_coll.to_str()
	    << "/" << new_coll_old_obj->oid.name
	    << " with " << obj->oid.name << dendl;
    delete new_coll_old_obj;
  }

  _do_coll_move(new_coll->m_coll, orig_coll->m_coll, *obj);
}

void DeterministicOpSequence::do_coll_rename(rngen_t& gen)
{
  int coll_pos = _gen_coll_id(gen);
  dout(0) << "do_coll_rename coll pos #" << coll_pos << dendl;

  coll_entry_t *coll_entry = get_coll_at(coll_pos);
  if (!coll_entry) {
    dout(0) << "do_coll_rename no collection at pos #" << coll_pos << dendl;
    return;
  }

  coll_t orig_coll = coll_entry->m_coll;
  char buf[100];
  memset(buf, 0, 100);
  snprintf(buf, 100, "0.%d_head", m_next_coll_nr++);
  coll_t new_coll(buf);
  coll_entry->m_coll = new_coll;

  dout(0) << "do_coll_rename " << orig_coll.to_str()
      << " => " << new_coll.to_str() << dendl;
  _do_coll_rename(orig_coll, new_coll);
}

void DeterministicOpSequence::do_coll_add(rngen_t& gen)
{
  coll_entry_t *orig_coll = NULL, *new_coll = NULL;
  if (!_prepare_colls(gen, orig_coll, new_coll))
    return;

  if (!orig_coll || !new_coll)
    return;

  boost::uniform_int<> obj_rng(0, orig_coll->m_objects.size()-1);
  int obj_pos = obj_rng(gen);
  int obj_key = -1;
  hobject_t *obj = orig_coll->get_obj_at(obj_pos, &obj_key);
  if (!obj) {
    dout(0) << "do_coll_add coll " << orig_coll->m_coll.to_str()
        << " has no object as pos #" << obj_pos << " (key " << obj_key << ")"
        << dendl;
    return;
  }
  dout(0) << "do_coll_add " << orig_coll->m_coll.to_str() << "/" << obj->oid.name
        << " => " << new_coll->m_coll.to_str() << "/" << obj->oid.name << dendl;
  new_coll->touch_obj(obj_key);

  _do_coll_add(orig_coll->m_coll, new_coll->m_coll, *obj);
}

void DeterministicOpSequence::_do_touch(coll_t coll, hobject_t& obj)
{
  ObjectStore::Transaction t;
  note_txn(&t);
  t.touch(coll, obj);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_remove(coll_t coll, hobject_t& obj)
{
  ObjectStore::Transaction t;
  note_txn(&t);
  t.remove(coll, obj);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_set_attrs(coll_t coll,
					    hobject_t &obj,
					    const map<string, bufferlist> &attrs)
{
  ObjectStore::Transaction t;
  note_txn(&t);
  t.omap_setkeys(coll, obj, attrs);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_write(coll_t coll, hobject_t& obj,
					uint64_t off, uint64_t len, const bufferlist& data)
{
  ObjectStore::Transaction t;
  note_txn(&t);
  t.write(coll, obj, off, len, data);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_clone(coll_t coll, hobject_t& orig_obj,
					hobject_t& new_obj)
{
  ObjectStore::Transaction t;
  note_txn(&t);
  t.clone(coll, orig_obj, new_obj);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_clone_range(coll_t coll,
					      hobject_t& orig_obj, hobject_t& new_obj, uint64_t srcoff,
					      uint64_t srclen, uint64_t dstoff)
{
  ObjectStore::Transaction t;
  note_txn(&t);
  t.clone_range(coll, orig_obj, new_obj, srcoff, srclen, dstoff);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_write_and_clone_range(coll_t coll,
                                                        hobject_t& orig_obj,
                                                        hobject_t& new_obj,
                                                        uint64_t srcoff,
                                                        uint64_t srclen,
                                                        uint64_t dstoff,
                                                        bufferlist& bl)
{
  ObjectStore::Transaction t;
  note_txn(&t);
  t.write(coll, orig_obj, srcoff, bl.length(), bl);
  t.clone_range(coll, orig_obj, new_obj, srcoff, srclen, dstoff);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_coll_move(coll_t new_coll,
					    coll_t old_coll, hobject_t& obj)
{
  ObjectStore::Transaction t;
  note_txn(&t);
  t.remove(new_coll, obj);
  t.collection_move(new_coll, old_coll, obj);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_coll_add(coll_t orig_coll, coll_t new_coll,
					   hobject_t& obj)
{
  ObjectStore::Transaction t;
  note_txn(&t);
  t.remove(new_coll, obj);
  t.collection_add(new_coll, orig_coll, obj);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_coll_rename(coll_t orig_coll, coll_t new_coll)
{
  ObjectStore::Transaction t;
  note_txn(&t);
  t.collection_rename(orig_coll, new_coll);
  m_store->apply_transaction(t);
}
