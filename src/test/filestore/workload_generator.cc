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

void usage(const char *name);

boost::scoped_ptr<WorkloadGenerator> wrkldgen;
const coll_t WorkloadGenerator::META_COLL("meta");
const coll_t WorkloadGenerator::TEMP_COLL("temp");

#define dout_subsys ceph_subsys_

WorkloadGenerator::WorkloadGenerator(vector<const char*> args) :
    m_destroy_coll_every_nr_runs(def_destroy_coll_every_nr_runs),
    m_num_colls(def_num_colls), m_num_obj_per_coll(def_num_obj_per_coll),
    m_store(0), m_nr_runs(0),
    m_in_flight(0), m_lock("State Lock"), m_next_coll_nr(0) {

  int err = 0;

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

  init();

  dout(0) << "#colls          = " << m_num_colls << dendl;
  dout(0) << "#objs per coll  = " << m_num_obj_per_coll << dendl;
  dout(0) << "#txs per destr  = " << m_destroy_coll_every_nr_runs << dendl;

}

void WorkloadGenerator::init_args(vector<const char*> args) {
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end();) {
    string val;

    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-num-colls", (char*) NULL)) {
      m_num_colls = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-objs-per-coll", (char*) NULL)) {
      m_num_obj_per_coll = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-destroy-coll-per-N-trans", (char*) NULL)) {
      m_destroy_coll_every_nr_runs = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_flag(args, i, "--help", (char*) NULL)) {
      usage(NULL);
      exit(0);
    }
  }
}

void WorkloadGenerator::init() {

  dout(0) << "Initializing..." << dendl;

  ObjectStore::Transaction *t;
  t = new ObjectStore::Transaction;

  t->create_collection(META_COLL);
  t->create_collection(TEMP_COLL);
  m_store->apply_transaction(*t);

  wait_for_ready();

  char buf[100];
  char meta_buf[100];
  for (int i = 0; i < m_num_colls; i++) {
    memset(buf, 0, 100);
    memset(meta_buf, 0, 100);
    snprintf(buf, 100, "0.%d_head", i);
    snprintf(meta_buf, 100, "pglog_0.%d_head", i);
    coll_entry_t *entry = new coll_entry_t(i, buf, meta_buf);

    dout(0) << "Creating collection " << entry->coll.to_str() << dendl;

    t = new ObjectStore::Transaction;

    t->create_collection(entry->coll);
    t->touch(META_COLL, entry->meta_obj);

    m_store->queue_transaction(&(entry->osr), t,
        new C_WorkloadGeneratorOnReadable(this, t));
    m_in_flight++;

    m_collections.insert(entry);
    m_next_coll_nr++;
  }

  dout(0) << "Currently in-flight collections: " << m_in_flight << dendl;

  wait_for_done();
  m_nr_runs = 0;
  dout(0) << "Done initializing!" << dendl;
}

int WorkloadGenerator::get_uniform_random_value(int min, int max) {
  boost::uniform_int<> value(min, max);
  return value(m_rng);
}

WorkloadGenerator::coll_entry_t
*WorkloadGenerator::get_rnd_coll_entry(bool erase = false) {
  set<WorkloadGenerator::coll_entry_t*>::iterator i = m_collections.begin();
  int index = get_uniform_random_value(0, m_collections.size()-1);
  for ( ; index > 0; --index, ++i) ;

  WorkloadGenerator::coll_entry_t *entry = *i;
  if (erase)
    m_collections.erase(i);
  return entry;
}

int WorkloadGenerator::get_random_collection_nr() {
  return (rand() % m_num_colls);
}

int WorkloadGenerator::get_random_object_nr(int coll_nr) {
  return ((rand() % m_num_obj_per_coll) + (coll_nr * m_num_obj_per_coll));
}

coll_t WorkloadGenerator::get_collection_by_nr(int nr) {
  char buf[100];
  memset(buf, 0, 100);

  snprintf(buf, 100, "0.%d_head", nr);
  return coll_t(buf);
}

hobject_t WorkloadGenerator::get_object_by_nr(int nr) {
  char buf[100];
  memset(buf, 0, 100);
  snprintf(buf, 100, "%d", nr);

  return hobject_t(sobject_t(object_t(buf), CEPH_NOSNAP));
}

hobject_t WorkloadGenerator::get_coll_meta_object(coll_t coll) {
  char buf[100];
  memset(buf, 0, 100);
  snprintf(buf, 100, "pglog_%s", coll.c_str());

  return hobject_t(sobject_t(object_t(buf), CEPH_NOSNAP));
}

/**
 * We'll generate a random amount of bytes, ranging from a single byte up to
 * a couple of MB.
 */
size_t WorkloadGenerator::get_random_byte_amount(size_t min, size_t max) {
  size_t diff = max - min;
  return (size_t) (min + (rand() % diff));
}

void WorkloadGenerator::get_filled_byte_array(bufferlist& bl, size_t size) {
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
    coll_t coll, hobject_t obj) {

  size_t bytes = get_random_byte_amount(min_write_bytes, max_write_bytes);
  bufferlist bl;
  get_filled_byte_array(bl, bytes);
  t->write(coll, obj, 0, bl.length(), bl);
}

void WorkloadGenerator::do_setattr_object(ObjectStore::Transaction *t,
    coll_t coll, hobject_t obj) {

  size_t size;
  size = get_random_byte_amount(min_xattr_obj_bytes, max_xattr_obj_bytes);

  bufferlist bl;
  get_filled_byte_array(bl, size);
  t->setattr(coll, obj, "objxattr", bl);
}

void WorkloadGenerator::do_setattr_collection(ObjectStore::Transaction *t,
    coll_t coll) {

  size_t size;
  size = get_random_byte_amount(min_xattr_coll_bytes, max_xattr_coll_bytes);

  bufferlist bl;
  get_filled_byte_array(bl, size);
  t->collection_setattr(coll, "collxattr", bl);
}

void WorkloadGenerator::do_append_log(ObjectStore::Transaction *t,
    coll_t coll) {

  bufferlist bl;
  get_filled_byte_array(bl, log_append_bytes);
  hobject_t log_obj = get_coll_meta_object(coll);

  struct stat st;
  int err = m_store->stat(META_COLL, log_obj, &st);
//  dout(0) << "stat return: " << err << dendl;
  assert(err >= 0);
  t->write(META_COLL, log_obj, st.st_size, bl.length(), bl);
}

void WorkloadGenerator::do_destroy_collection(ObjectStore::Transaction *t,
    WorkloadGenerator::coll_entry_t *entry) {

  m_nr_runs = 0;
  entry->osr.flush();
  vector<hobject_t> ls;
  m_store->collection_list(entry->coll, ls);
  dout(0) << "Destroying collection '" << entry->coll.to_str()
      << "' (" << ls.size() << " objects)" << dendl;

  vector<hobject_t>::iterator it;
  for (it = ls.begin(); it < ls.end(); it++) {
    t->remove(entry->coll, *it);
  }

  t->remove_collection(entry->coll);
  t->remove(META_COLL, entry->meta_obj);
}

void WorkloadGenerator::do_create_collection(ObjectStore::Transaction *t) {

}

void WorkloadGenerator::run() {

  do {
    if (!m_collections.size()) {
      dout(0) << "We ran out of collections!" << dendl;
      break;
    }

    m_lock.Lock();
    wait_for_ready();

    ObjectStore::Transaction *t = new ObjectStore::Transaction;

    bool destroy_collection = should_destroy_collection();
    coll_entry_t *entry = get_rnd_coll_entry(destroy_collection);

    Context *c;
    if (destroy_collection) {
      do_destroy_collection(t, entry);
      c = new C_WorkloadGeneratorOnDestroyed(this, t, entry);
    } else {
      int obj_nr = get_random_object_nr(entry->id);
      hobject_t obj = get_object_by_nr(obj_nr);

      do_write_object(t, entry->coll, obj);
      do_setattr_object(t, entry->coll, obj);
      do_setattr_collection(t, entry->coll);
      do_append_log(t, entry->coll);
      c = new C_WorkloadGeneratorOnReadable(this, t);
    }

    m_store->queue_transaction(&(entry->osr), t, c);

    m_in_flight++;

    m_lock.Unlock();
  } while (true);
}

void WorkloadGenerator::print_results() {

}

void usage(const char *name) {
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
                                      destroying a collection.\
    " << std::endl;
}

int main(int argc, const char *argv[]) {
  vector<const char*> def_args;
  vector<const char*> args;
  def_args.push_back("--osd-journal-size");
  def_args.push_back("400");
  def_args.push_back("--osd-data");
  def_args.push_back("workload_gen_dir");
  def_args.push_back("--osd-journal");
  def_args.push_back("workload_gen_journal");
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
