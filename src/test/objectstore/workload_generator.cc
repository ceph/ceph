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
#include <cctype>
#include <errno.h>
#include <sys/time.h>
#include "os/ObjectStore.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/debug.h"
#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include "workload_generator.h"
#include "include/assert.h"

#include "TestObjectStoreState.h"

static const char *our_name = NULL;
void usage();

boost::scoped_ptr<WorkloadGenerator> wrkldgen;

#define dout_subsys ceph_subsys_


WorkloadGenerator::WorkloadGenerator(vector<const char*> args)
  : TestObjectStoreState(NULL),
    m_max_in_flight(def_max_in_flight),
    m_num_ops(-1),
    m_destroy_coll_every_nr_runs(def_destroy_coll_every_nr_runs),
    m_num_colls(def_num_colls),
    m_write_data_bytes(0), m_write_xattr_obj_bytes(0),
    m_write_xattr_coll_bytes(0), m_write_pglog_bytes(0),
    m_suppress_write_data(false), m_suppress_write_xattr_obj(false),
    m_suppress_write_xattr_coll(false), m_suppress_write_log(false),
    m_do_stats(false),
    m_stats_finished_txs(0),
    m_stats_lock("WorldloadGenerator::m_stats_lock"),
    m_stats_show_secs(5),
    m_stats_total_written(0),
    m_stats_begin()
{
  int err = 0;

  m_nr_runs.set(0);

  init_args(args);
  dout(0) << "data            = " << g_conf->osd_data << dendl;
  dout(0) << "journal         = " << g_conf->osd_journal << dendl;
  dout(0) << "journal size    = " << g_conf->osd_journal_size << dendl;

  err = ::mkdir(g_conf->osd_data.c_str(), 0755);
  ceph_assert(err == 0 || (err < 0 && errno == EEXIST));
  ObjectStore *store_ptr = ObjectStore::create(g_ceph_context,
                                               g_conf->osd_objectstore,
                                               g_conf->osd_data,
                                               g_conf->osd_journal);
  m_store.reset(store_ptr);
  err = m_store->mkfs();
  ceph_assert(err == 0);
  err = m_store->mount();
  ceph_assert(err == 0);

  set_max_in_flight(m_max_in_flight);
  set_num_objs_per_coll(def_num_obj_per_coll);

  init(m_num_colls, 0);

  dout(0) << "#colls          = " << m_num_colls << dendl;
  dout(0) << "#objs per coll  = " << m_num_objs_per_coll << dendl;
  dout(0) << "#txs per destr  = " << m_destroy_coll_every_nr_runs << dendl;

}

size_t WorkloadGenerator::_parse_size_or_die(std::string& val)
{
  size_t s = 0;
  int multiplier = 0;
  size_t i = 0;

  if (val.empty()) // this should never happen, but catch it anyway.
    goto die;


  for (i = 0; i < val.length(); i++) {
    if (!isdigit(val[i])) {
      if (isalpha(val[i])) {
        val[i] = tolower(val[i]);
        switch (val[i]) {
        case 'b': break;
        case 'k': multiplier = 10; break;
        case 'm': multiplier = 20; break;
        case 'g': multiplier = 30; break;
        default:
          goto die;
        }
        val[i] = '\0';
        break;
      } else {
        goto die;
      }
    }
  }

  s = strtoll(val.c_str(), NULL, 10) * (1 << multiplier);
  return s;

die:
  usage();
  exit(1);
}

void WorkloadGenerator::_suppress_ops_or_die(std::string& val)
{
  for (size_t i = 0; i < val.length(); i++) {
    switch (val[i]) {
    case 'c': m_suppress_write_xattr_coll = true; break;
    case 'o': m_suppress_write_xattr_obj = true; break;
    case 'l': m_suppress_write_log = true; break;
    case 'd': m_suppress_write_data = true; break;
    default:
      usage();
      exit(1);
    }
  }
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
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-max-in-flight", (char*) NULL)) {
      m_max_in_flight = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-write-data-size", (char*) NULL)) {
      m_write_data_bytes = _parse_size_or_die(val);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-write-xattr-obj-size", (char*) NULL)) {
      m_write_xattr_obj_bytes = _parse_size_or_die(val);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-write-xattr-coll-size", (char*) NULL)) {
      m_write_xattr_coll_bytes = _parse_size_or_die(val);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-write-pglog-size", (char*) NULL)) {
      m_write_pglog_bytes = _parse_size_or_die(val);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-suppress-ops", (char*) NULL)) {
      _suppress_ops_or_die(val);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-show-stats-period", (char*) NULL)) {
      m_stats_show_secs = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_flag(args, i, "--test-show-stats", (char*) NULL)) {
      m_do_stats = true;
    } else if (ceph_argparse_flag(args, i, "--help", (char*) NULL)) {
      usage();
      exit(0);
    }
  }
}

int WorkloadGenerator::get_uniform_random_value(int min, int max)
{
  boost::uniform_int<> value(min, max);
  return value(m_rng);
}

TestObjectStoreState::coll_entry_t *WorkloadGenerator::get_rnd_coll_entry(bool erase = false)
{
  int index = get_uniform_random_value(0, m_collections_ids.size()-1);
  coll_entry_t *entry = get_coll_at(index, erase);
  return entry;
}

hobject_t *WorkloadGenerator::get_rnd_obj(coll_entry_t *entry)
{
  assert(entry != NULL);

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
  if (false) {
    for (unsigned int i = 0; i < size - 1; i++) {
      bp[i] = alphanum[rand() % sizeof(alphanum)];
    }
    bp[size - 1] = '\0';
  } else {
    bp.zero();
  }
  bl.append(bp);
}

void WorkloadGenerator::do_write_object(ObjectStore::Transaction *t,
					coll_t coll, hobject_t obj,
					C_StatState *stat)
{
  if (m_suppress_write_data) {
    dout(5) << __func__ << " suppressed" << dendl;
    return;
  }

  size_t size = m_write_data_bytes;
  if (!size)
    size = get_random_byte_amount(min_write_bytes, max_write_bytes);

  bufferlist bl;
  get_filled_byte_array(bl, size);

  dout(2) << __func__ << " " << coll << "/" << obj
	  << " size " << bl.length() << dendl;

  if (m_do_stats && (stat != NULL))
    stat->written_data += bl.length();

  t->write(coll, ghobject_t(obj), 0, bl.length(), bl);
}

void WorkloadGenerator::do_setattr_object(ObjectStore::Transaction *t,
					  coll_t coll, hobject_t obj,
					  C_StatState *stat)
{
  if (m_suppress_write_xattr_obj) {
    dout(5) << __func__ << " suppressed" << dendl;
    return;
  }

  size_t size = m_write_xattr_obj_bytes;
  if (!size)
    size = get_random_byte_amount(min_xattr_obj_bytes, max_xattr_obj_bytes);

  bufferlist bl;
  get_filled_byte_array(bl, size);

  dout(2) << __func__ << " " << coll << "/" << obj << " size " << size << dendl;

  if (m_do_stats && (stat != NULL))
      stat->written_data += bl.length();

  t->setattr(coll, ghobject_t(obj), "objxattr", bl);
}

void WorkloadGenerator::do_pgmeta_omap_set(ObjectStore::Transaction *t, spg_t pgid,
					   coll_t coll, C_StatState *stat)
{
  if (m_suppress_write_xattr_coll) {
    dout(5) << __func__ << " suppressed" << dendl;
    return;
  }

  size_t size = m_write_xattr_coll_bytes;
  if (!size)
    size = get_random_byte_amount(min_xattr_coll_bytes, max_xattr_coll_bytes);

  bufferlist bl;
  get_filled_byte_array(bl, size);
  dout(2) << __func__ << " coll " << coll << " size " << size << dendl;

  if (m_do_stats && (stat != NULL))
      stat->written_data += bl.length();

  ghobject_t pgmeta(pgid.make_pgmeta_oid());
  map<string,bufferlist> values;
  values["_"].claim(bl);
  t->omap_setkeys(coll, pgmeta, values);
}


void WorkloadGenerator::do_append_log(ObjectStore::Transaction *t,
                                      coll_entry_t *entry, C_StatState *stat)
{
  if (m_suppress_write_log) {
    dout(5) << __func__ << " suppressed" << dendl;
    return;
  }

  size_t size = (m_write_pglog_bytes ? m_write_pglog_bytes : log_append_bytes);

  bufferlist bl;
  get_filled_byte_array(bl, size);
  ghobject_t log_obj = entry->m_meta_obj;

  dout(2) << __func__ << " coll " << entry->m_coll << " "
      << coll_t::meta() << " /" << log_obj << " (" << bl.length() << ")" << dendl;

  if (m_do_stats && (stat != NULL))
      stat->written_data += bl.length();

  uint64_t s = pg_log_size[entry->m_coll];
  t->write(coll_t::meta(), log_obj, s, bl.length(), bl);
  pg_log_size[entry->m_coll] += bl.length();
}

void WorkloadGenerator::do_destroy_collection(ObjectStore::Transaction *t,
					      coll_entry_t *entry,
					      C_StatState *stat)
{  
  m_nr_runs.set(0);
  entry->m_osr.flush();
  vector<ghobject_t> ls;
  m_store->collection_list(entry->m_coll, ghobject_t(), ghobject_t::get_max(),
			   true, INT_MAX, &ls, NULL);
  dout(2) << __func__ << " coll " << entry->m_coll
      << " (" << ls.size() << " objects)" << dendl;

  for (vector<ghobject_t>::iterator it = ls.begin(); it < ls.end(); ++it) {
    t->remove(entry->m_coll, *it);
  }

  t->remove_collection(entry->m_coll);
  t->remove(coll_t::meta(), entry->m_meta_obj);
}

TestObjectStoreState::coll_entry_t
*WorkloadGenerator::do_create_collection(ObjectStore::Transaction *t,
                                         C_StatState *stat)
{
  coll_entry_t *entry = coll_create(m_next_coll_nr++);
  if (!entry) {
    dout(0) << __func__ << " failed to create coll id "
        << m_next_coll_nr << dendl;
    return NULL;
  }
  m_collections.insert(make_pair(entry->m_id, entry));

  dout(2) << __func__ << " id " << entry->m_id << " coll " << entry->m_coll << dendl;
  t->create_collection(entry->m_coll, 32);
  dout(2) << __func__ << " meta " << coll_t::meta() << "/" << entry->m_meta_obj << dendl;
  t->touch(coll_t::meta(), entry->m_meta_obj);
  return entry;
}

void WorkloadGenerator::do_stats()
{
  utime_t now = ceph_clock_now(NULL);
  m_stats_lock.Lock();

  utime_t duration = (now - m_stats_begin);

  // when cast to double, a utime_t behaves properly
  double throughput = (m_stats_total_written / ((double) duration));
  double tx_throughput (m_stats_finished_txs / ((double) duration));

  dout(0) << __func__
	  << " written: " << m_stats_total_written
	  << " duration: " << duration << " sec"
	  << " bandwidth: " << prettybyte_t(throughput) << "/s"
	  << " iops: " << tx_throughput << "/s"
	  << dendl;

  m_stats_lock.Unlock();
}

void WorkloadGenerator::run()
{
  bool create_coll = false;
  int ops_run = 0;

  utime_t stats_interval(m_stats_show_secs, 0);
  utime_t now = ceph_clock_now(NULL);
  utime_t stats_time = now;
  m_stats_begin = now;

  do {
    C_StatState *stat_state = NULL;

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
    TestObjectStoreState::coll_entry_t *entry = NULL;


    if (m_do_stats) {
      utime_t now = ceph_clock_now(NULL);
      utime_t elapsed = now - stats_time;
      if (elapsed >= stats_interval) {
	do_stats();
	stats_time = now;
      }
      stat_state = new C_StatState(this, now);
    }

    if (create_coll) {
      create_coll = false;

      entry = do_create_collection(t, stat_state);
      if (!entry) {
        dout(0) << __func__ << " something went terribly wrong creating coll" << dendl;
        break;
      }

      c = new C_OnReadable(this);
      goto queue_tx;
    }

    destroy_collection = should_destroy_collection();
    entry = get_rnd_coll_entry(destroy_collection);
    assert(entry != NULL);

    if (destroy_collection) {
      do_destroy_collection(t, entry, stat_state);
      c = new C_OnDestroyed(this, entry);
      if (!m_num_ops)
        create_coll = true;
    } else {
      hobject_t *obj = get_rnd_obj(entry);

      do_write_object(t, entry->m_coll, *obj, stat_state);
      do_setattr_object(t, entry->m_coll, *obj, stat_state);
      do_pgmeta_omap_set(t, entry->m_pgid, entry->m_coll, stat_state);
      do_append_log(t, entry, stat_state);

      c = new C_OnReadable(this);
    }

queue_tx:

    if (m_do_stats) {
      Context *tmp = c;
      c = new C_StatWrapper(stat_state, tmp);
    }

    m_store->queue_transaction(&(entry->m_osr), std::move(*t), c);
    delete t;

    inc_in_flight();

    ops_run ++;

  } while (true);

  dout(2) << __func__ << " waiting for "
	  << m_in_flight.read() << " in-flight transactions" << dendl;

  wait_for_done();

  do_stats();

  dout(0) << __func__ << " finishing" << dendl;
}

void usage()
{
  cout << "usage: " << our_name << "[options]" << std::endl;

  cout << "\
\n\
Global Options:\n\
  -c FILE                             Read configuration from FILE\n\
  --osd-objectstore TYPE              Set OSD ObjectStore type\n\
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
                                      (a VAL of 0 runs the test forever)\n\
   --test-max-in-flight VAL           Maximum number of in-flight transactions\n\
                                      (default: 50)\n\
   --test-suppress-ops OPS            Suppress ops specified in OPS\n\
   --test-write-data-size SIZE        Specify SIZE for all data writes\n\
   --test-write-xattr-obj-size SIZE   Specify SIZE for all xattrs on objects\n\
   --test-write-xattr-coll-size SIZE  Specify SIZE for all xattrs on colls\n\
   --test-write-pglog-size SIZE       Specify SIZE for all pglog writes\n\
   --test-show-stats                  Show stats as we go\n\
   --test-show-stats-period SECS      Show stats every SECS (default: 5)\n\
\n\
   SIZE is a numeric value that can be assumed as being bytes, or may be any\n\
   other unit if specified: B or b, K or k, M or m, G or g.\n\
      e.g., 1G = 1024M = 1048576k = 1073741824\n\
\n\
   OPS can be one or more of the following options:\n\
      c    writes on collection's xattrs\n\
      o    writes on object's xattr\n\
      l    writes on pglog\n\
      d    data writes on objects\n\
\n\
" << std::endl;
}

int main(int argc, const char *argv[])
{
  vector<const char*> def_args;
  vector<const char*> args;

  our_name = argv[0];

  def_args.push_back("--osd-journal-size");
  def_args.push_back("400");
//  def_args.push_back("--osd-data");
//  def_args.push_back("workload_gen_dir");
//  def_args.push_back("--osd-journal");
//  def_args.push_back("workload_gen_dir/journal");
  argv_to_vec(argc, argv, args);

  auto cct = global_init(&def_args, args,
			 CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  WorkloadGenerator *wrkldgen_ptr = new WorkloadGenerator(args);
  wrkldgen.reset(wrkldgen_ptr);
  wrkldgen->run();
  return 0;
}
