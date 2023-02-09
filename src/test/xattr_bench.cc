// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <stdio.h>
#include <time.h>
#include <string.h>
#include <iostream>
#include <iterator>
#include <sstream>
#include "os/bluestore/BlueStore.h"
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "global/global_init.h"
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/binomial_distribution.hpp>
#include <gtest/gtest.h>

#include "include/unordered_map.h"

void usage(const string &name) {
  std::cerr << "Usage: " << name << " [xattr|omap] store_path"
	    << std::endl;
}

const int THREADS = 5;

template <typename T>
typename T::iterator rand_choose(T &cont) {
  if (std::empty(cont) == 0) {
    return std::end(cont);
  }
  return std::next(std::begin(cont), rand() % cont.size());
}

class OnApplied : public Context {
public:
  ceph::mutex *lock;
  ceph::condition_variable *cond;
  int *in_progress;
  ObjectStore::Transaction *t;
  OnApplied(ceph::mutex *lock,
	    ceph::condition_variable *cond,
	    int *in_progress,
	    ObjectStore::Transaction *t)
    : lock(lock), cond(cond),
      in_progress(in_progress), t(t) {
    std::lock_guard l{*lock};
    (*in_progress)++;
  }

  void finish(int r) override {
    std::lock_guard l{*lock};
    (*in_progress)--;
    cond->notify_all();
  }
};

uint64_t get_time() {
  time_t start;
  time(&start);
  return start * 1000;
}

double print_time(uint64_t ms) {
  return ((double)ms)/1000;
}

uint64_t do_run(ObjectStore *store, int attrsize, int numattrs,
		int run,
		int transsize, int ops,
		ostream &out) {
  ceph::mutex lock = ceph::make_mutex("lock");
  ceph::condition_variable cond;
  int in_flight = 0;
  ObjectStore::Sequencer osr(__func__);
  ObjectStore::Transaction t;
  map<coll_t, pair<set<string>, ObjectStore::Sequencer*> > collections;
  for (int i = 0; i < 3*THREADS; ++i) {
    coll_t coll(spg_t(pg_t(0, i + 1000*run), shard_id_t::NO_SHARD));
    t.create_collection(coll, 0);
    set<string> objects;
    for (int i = 0; i < transsize; ++i) {
      stringstream obj_str;
      obj_str << i;
      t.touch(coll,
	      ghobject_t(hobject_t(sobject_t(obj_str.str(), CEPH_NOSNAP))));
      objects.insert(obj_str.str());
    }
    collections[coll] = make_pair(objects, new ObjectStore::Sequencer(coll.to_str()));
  }
  store->queue_transaction(&osr, std::move(t));

  bufferlist bl;
  for (int i = 0; i < attrsize; ++i) {
    bl.append('\0');
  }

  uint64_t start = get_time();
  for (int i = 0; i < ops; ++i) {
    {
      std::unique_lock l{lock};
      cond.wait(l, [&] { in_flight < THREADS; });
    }
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    map<coll_t, pair<set<string>, ObjectStore::Sequencer*> >::iterator iter =
      rand_choose(collections);
    for (set<string>::iterator obj = iter->second.first.begin();
	 obj != iter->second.first.end();
	 ++obj) {
      for (int j = 0; j < numattrs; ++j) {
	stringstream ss;
	ss << i << ", " << j << ", " << *obj;
	t->setattr(iter->first,
		   ghobject_t(hobject_t(sobject_t(*obj, CEPH_NOSNAP))),
		   ss.str().c_str(),
		   bl);
      }
    }
    store->queue_transaction(iter->second.second, std::move(*t),
			     new OnApplied(&lock, &cond, &in_flight,
					   t));
    delete t;
  }
  {
    std::unique_lock l{lock};
    cond.wait(l, [&] { return in_flight == 0; });
  }
  return get_time() - start;
}

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage(argv[0]);
    exit(0);
  }

  auto cct = global_init(0, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  std::cerr << "args: " << args << std::endl;
  if (args.size() < 2) {
    usage(argv[0]);
    return 1;
  }

  string store_path(args[1]);

  boost::scoped_ptr<ObjectStore> store(new BlueStore(cct.get(), store_path));

  std::cerr << "mkfs starting" << std::endl;
  ceph_assert(!store->mkfs());
  ceph_assert(!store->mount());
  std::cerr << "mounted" << std::endl;

  std::cerr << "attrsize\tnumattrs\ttranssize\tops\ttime" << std::endl;
  int runs = 0;
  int total_size = 11;
  for (int i = 6; i < total_size; ++i) {
    for (int j = (total_size - i); j >= 0; --j) {
      std::cerr << "starting run " << runs << std::endl;
      ++runs;
      uint64_t time = do_run(store.get(), (1 << i), (1 << j), runs,
			     10,
			     1000, std::cout);
      std::cout << (1 << i) << "\t"
		<< (1 << j) << "\t"
		<< 10 << "\t"
		<< 1000 << "\t"
		<< print_time(time) << std::endl;
    }
  }
  store->umount();
  return 0;
}
