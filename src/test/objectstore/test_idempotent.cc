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

#include <iostream>
#include <sstream>
#include <boost/scoped_ptr.hpp>
#include "os/filestore/FileStore.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "test/common/ObjectContents.h"
#include "FileStoreTracker.h"
#include "kv/KeyValueDB.h"
#include "os/ObjectStore.h"

void usage(const string &name) {
  std::cerr << "Usage: " << name << " [new|continue] store_path store_journal db_path"
	    << std::endl;
}

template <typename T>
typename T::iterator rand_choose(T &cont) {
  if (cont.size() == 0) {
    return cont.end();
  }
  int index = rand() % cont.size();
  typename T::iterator retval = cont.begin();

  for (; index > 0; --index) ++retval;
  return retval;
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  std::cerr << "args: " << args << std::endl;
  if (args.size() < 4) {
    usage(argv[0]);
    return 1;
  }

  string store_path(args[1]);
  string store_dev(args[2]);
  string db_path(args[3]);

  bool start_new = false;
  if (string(args[0]) == string("new")) start_new = true;

  KeyValueDB *_db = KeyValueDB::create(g_ceph_context, "leveldb", db_path);
  assert(!_db->create_and_open(std::cerr));
  boost::scoped_ptr<KeyValueDB> db(_db);
  boost::scoped_ptr<ObjectStore> store(new FileStore(store_path, store_dev));

  ObjectStore::Sequencer osr(__func__);
  coll_t coll(spg_t(pg_t(0,12),shard_id_t::NO_SHARD));

  if (start_new) {
    std::cerr << "mkfs" << std::endl;
    assert(!store->mkfs());
    ObjectStore::Transaction t;
    assert(!store->mount());
    t.create_collection(coll, 0);
    store->apply_transaction(&osr, std::move(t));
  } else {
    assert(!store->mount());
  }

  FileStoreTracker tracker(store.get(), db.get());

  set<string> objects;
  for (unsigned i = 0; i < 10; ++i) {
    stringstream stream;
    stream << "Object_" << i;
    tracker.verify(coll, stream.str(), true);
    objects.insert(stream.str());
  }

  while (1) {
    FileStoreTracker::Transaction t;
    for (unsigned j = 0; j < 100; ++j) {
      int val = rand() % 100;
      if (val < 30) {
	t.write(coll, *rand_choose(objects));
      } else if (val < 60) {
	t.clone(coll, *rand_choose(objects),
		*rand_choose(objects));
      } else if (val < 70) {
	t.remove(coll, *rand_choose(objects));
      } else {
	t.clone_range(coll, *rand_choose(objects),
		      *rand_choose(objects));
      }
    }
    tracker.submit_transaction(t);
    tracker.verify(coll, *rand_choose(objects));
  }
  return 0;
}
