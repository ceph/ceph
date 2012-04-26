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
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <assert.h>
#include <time.h>
#include <stdlib.h>
#include <signal.h>
#include "os/FileStore.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/debug.h"
#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include "workload_generator.h"
#include "common/debug.h"

#include "TestFileStoreState.h"

void usage(const char *name);

boost::scoped_ptr<WorkloadGenerator> wrkldgen;

#define dout_subsys ceph_subsys_

WorkloadGenerator::WorkloadGenerator(vector<const char*> args)
  : TestFileStoreState(NULL),
    m_num_ops(-1),
    m_destroy_coll_every_nr_runs(def_destroy_coll_every_nr_runs),
    m_num_colls(def_num_colls)
{
  int err = 0;

  m_nr_runs.set(0);

  init_args(args);
  dout(0) << "data            = " << g_conf->osd_data << dendl;
  dout(0) << "journal         = " << g_conf->osd_journal << dendl;
  dout(0) << "journal size    = " << g_conf->osd_journal_size << dendl;

  ::mkdir(g_conf->osd_data.c_str(), 0755);
  ObjectStore *store_ptr = new FileStore(g_conf->osd_data, g_conf->osd_journal);
  m_store.reset(store_ptr);
  err = m_store->mkfs();
  ceph_assert(err == 0);
  err = m_store->mount();
  ceph_assert(err == 0);

  set_max_in_flight(max_in_flight);
  set_num_objs_per_coll(def_num_obj_per_coll);

  init(m_num_colls, 0);

  dout(0) << "#colls          = " << m_num_colls << dendl;
  dout(0) << "#objs per coll  = " << m_num_objs_per_coll << dendl;
  dout(0) << "#txs per destr  = " << m_destroy_coll_every_nr_runs << dendl;

}

void WorkloadGenerator::init_args(vector<const char*> args)
{
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end();) {
    string val;

    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-num-colls", (char*) NULL)) {
      m_num_colls = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-objs-per-coll", (char*) NULL)) {
      m_num_objs_per_coll = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-destroy-coll-per-N-trans", (char*) NULL)) {
      m_destroy_coll_every_nr_runs = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-num-ops", (char*) NULL)) {
      m_num_ops = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_flag(args, i, "--help", (char*) NULL)) {
      usage(NULL);
      exit(0);
    }
  }
}

int WorkloadGenerator::get_uniform_random_value(int min, int max)
{
  boost::uniform_int<> value(min, max);
  return value(m_rng);
}

TestFileStoreState::coll_entry_t *WorkloadGenerator::get_rnd_coll_entry(bool erase = false)
{
  int index = get_uniform_random_value(0, m_collections.size()-1);
  coll_entry_t *entry = get_coll_at(index, erase);
  return entry;
}

hobject_t *WorkloadGenerator::get_rnd_obj(coll_entry_t *entry)
{
  bool create =
      (get_uniform_random_value(0,100) < 50 || !entry->m_objects.size());

  if (create && ((int) entry->m_objects.size() < m_num_objs_per_coll)) {
    return (entry->touch_obj(entry->m_next_object_id++));
  }

  int idx = get_uniform_random_value(0, entry->m_objects.size()-1);
  return entry->get_obj_at(idx);
}

/**
 * We'll generate a random amount of bytes, ranging from a single byte up to
 * a couple of MB.
 */
size_t WorkloadGenerator::get_random_byte_amount(size_t min, size_t max)
{
  size_t diff = max - min;
  return (size_t) (min + (rand() % diff));
}

void WorkloadGenerator::get_filled_byte_array(bufferlist& bl, size_t size)
{
  static const char alphanum[] = "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

  bufferptr bp(size);
  for (unsigned int i = 0; i < size - 1; i++) {
    bp[i] = alphanum[rand() % sizeof(alphanum)];
  }
  bp[size - 1] = '\0';
  bl.append(bp);
}

void WorkloadGenerator::do_write_object(ObjectStore::Transaction *t,
					coll_t coll, hobject_t obj)
{
  size_t bytes = get_random_byte_amount(min_write_bytes, max_write_bytes);
  bufferlist bl;
  get_filled_byte_array(bl, bytes);

  dout(0) << __func__ << " " << coll << "/" << obj
      << " size " << bl.length() << dendl;
  t->write(coll, obj, 0, bl.length(), bl);
}

void WorkloadGenerator::do_setattr_object(ObjectStore::Transaction *t,
					  coll_t coll, hobject_t obj)
{
  size_t size;
  size = get_random_byte_amount(min_xattr_obj_bytes, max_xattr_obj_bytes);

  bufferlist bl;
  get_filled_byte_array(bl, size);

  dout(0) << __func__ << " " << coll << "/" << obj << " size " << size << dendl;
  t->setattr(coll, obj, "objxattr", bl);
}

void WorkloadGenerator::do_setattr_collection(ObjectStore::Transaction *t,
					      coll_t coll)
{
  size_t size;
  size = get_random_byte_amount(min_xattr_coll_bytes, max_xattr_coll_bytes);

  bufferlist bl;
  get_filled_byte_array(bl, size);
  dout(0) << __func__ << " coll " << coll << " size " << size << dendl;
  t->collection_setattr(coll, "collxattr", bl);
}

//void WorkloadGenerator::do_append_log(ObjectStore::Transaction *t,
//				      coll_t coll)
void WorkloadGenerator::do_append_log(ObjectStore::Transaction *t,
                                      coll_entry_t *entry)
{
  bufferlist bl;
  get_filled_byte_array(bl, log_append_bytes);
  hobject_t log_obj = entry->m_meta_obj;

  dout(0) << __func__ << " coll " << entry->m_coll << " "
      << META_COLL << " /" << log_obj << " (" << bl.length() << ")" << dendl;

  uint64_t s = pg_log_size[entry->m_coll];
  t->write(META_COLL, log_obj, s, bl.length(), bl);
  pg_log_size[entry->m_coll] += bl.length();
}

void WorkloadGenerator::do_destroy_collection(ObjectStore::Transaction *t,
					      coll_entry_t *entry)
{  
  m_nr_runs.set(0);
  entry->m_osr.flush();
  vector<hobject_t> ls;
  m_store->collection_list(entry->m_coll, ls);
  dout(0) << __func__ << " coll " << entry->m_coll
      << " (" << ls.size() << " objects)" << dendl;

  vector<hobject_t>::iterator it;
  for (it = ls.begin(); it < ls.end(); it++) {
    t->remove(entry->m_coll, *it);
  }

  t->remove_collection(entry->m_coll);
  t->remove(META_COLL, entry->m_meta_obj);
}

TestFileStoreState::coll_entry_t
*WorkloadGenerator::do_create_collection(ObjectStore::Transaction *t)
{
  coll_entry_t *entry = coll_create(m_next_coll_nr++);
  if (!entry) {
    dout(0) << __func__ << " failed to create coll id "
        << m_next_coll_nr << dendl;
    return NULL;
  }
  m_collections.insert(make_pair(entry->m_id, entry));

  dout(0) << __func__ << " id " << entry->m_id << " coll " << entry->m_coll << dendl;
  t->create_collection(entry->m_coll);
  dout(0) << __func__ << " meta " << META_COLL << "/" << entry->m_meta_obj << dendl;
  t->touch(META_COLL, entry->m_meta_obj);
  return entry;
}

void WorkloadGenerator::run()
{
  bool create_coll = false;
  int ops_run = 0;
  do {
    if (m_num_ops && (ops_run == m_num_ops))
      break;

    if (!create_coll && !m_collections.size()) {
      dout(0) << "We ran out of collections!" << dendl;
      break;
    }

    dout(5) << __func__
        << " m_finished_lock is-locked: " << m_finished_lock.is_locked()
        << " in-flight: " << m_in_flight.read()
        << dendl;

    wait_for_ready();

    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    Context *c;
    bool destroy_collection = false;
    TestFileStoreState::coll_entry_t *entry = NULL;

    if (create_coll) {
      create_coll = false;

      entry = do_create_collection(t);
      if (!entry) {
        dout(0) << __func__ << " something went terribly wrong creating coll" << dendl;
        break;
      }

      c = new C_OnReadable(this, t);
      goto queue_tx;
    }

    destroy_collection = should_destroy_collection();
    entry = get_rnd_coll_entry(destroy_collection);

    if (destroy_collection) {
      do_destroy_collection(t, entry);
      c = new C_OnDestroyed(this, t, entry);
      if (!m_num_ops)
        create_coll = true;
    } else {
      hobject_t *obj = get_rnd_obj(entry);

      do_write_object(t, entry->m_coll, *obj);
      do_setattr_object(t, entry->m_coll, *obj);
      do_setattr_collection(t, entry->m_coll);
      do_append_log(t, entry);

      c = new C_OnReadable(this, t);
    }

queue_tx:
    m_store->queue_transaction(&(entry->m_osr), t, c);

    inc_in_flight();

    ops_run ++;

  } while (true);

  dout(0) << __func__ << " waiting for "
      << m_in_flight.read() << " in-flight transactions" << dendl;

  wait_for_done();

  dout(0) << __func__ << " finishing" << dendl;
}

void WorkloadGenerator::print_results()
{

}

void usage(const char *name)
{
  if (name)
    cout << "usage: " << name << "[options]" << std::endl;

  cout << "\
\n\
Global Options:\n\
  -c FILE                             Read configuration from FILE\n\
  --osd-data PATH                     Set OSD Data path\n\
  --osd-journal PATH                  Set OSD Journal path\n\
  --osd-journal-size VAL              Set Journal size\n\
  --help                              This message\n\
\n\
Test-specific Options:\n\
  --test-num-colls VAL                Set the number of collections\n\
  --test-num-objs-per-coll VAL        Set the number of objects per collection\n\
  --test-destroy-coll-per-N-trans VAL Set how many transactions to run before\n\
                                      destroying a collection.\n\
  --test-num-ops VAL                  Run a certain number of operations\n\
                                      (a VAL of 0 runs the test forever)\
    " << std::endl;
}

int main(int argc, const char *argv[])
{
  vector<const char*> def_args;
  vector<const char*> args;
  def_args.push_back("--osd-journal-size");
  def_args.push_back("400");
  def_args.push_back("--osd-data");
  def_args.push_back("workload_gen_dir");
  def_args.push_back("--osd-journal");
  def_args.push_back("workload_gen_dir/journal");
  argv_to_vec(argc, argv, args);

  global_init(&def_args, args,
      CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  WorkloadGenerator *wrkldgen_ptr = new WorkloadGenerator(args);
  wrkldgen.reset(wrkldgen_ptr);
  wrkldgen->run();
  wrkldgen->print_results();
  return 0;
}
