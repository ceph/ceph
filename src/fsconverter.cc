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
#include "os/FileStore.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include <boost/scoped_ptr.hpp>

#include <ext/hash_map>
using __gnu_cxx::hash_map;

void usage() {
  cerr << "usage: fsconverter filestore_path journal_path" << std::endl;
  exit(0);
}

int convert_collection(ObjectStore *store, coll_t cid) {
  vector<hobject_t> objects;
  int r = store->collection_list(cid, objects);
  if (r < 0)
    return r;
  string temp_name("temp");
  map<string, bufferptr> aset;
  r = store->collection_getattrs(cid, aset);
  if (r < 0)
    return r;
  while (store->collection_exists(coll_t(temp_name)))
    temp_name = "_" + temp_name;
  coll_t temp(temp_name);
  
  ObjectStore::Transaction t;
  t.collection_rename(cid, temp);
  t.create_collection(cid);
  for (vector<hobject_t>::iterator obj = objects.begin();
       obj != objects.end();
       ++obj) {
    t.collection_add(cid, temp, *obj);
    t.collection_remove(temp, *obj);
  }
  for (map<string,bufferptr>::iterator i = aset.begin();
       i != aset.end();
       ++i) {
    bufferlist bl;
    bl.append(i->second);
    t.collection_setattr(cid, i->first, bl);
  }
  t.remove_collection(temp);
  r = store->apply_transaction(t);
  if (r < 0)
    return r;
  store->sync_and_flush();
  store->sync();
  return 0;
}

int main(int argc, const char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  // args
  if (args.size() != 2) 
    usage();

  boost::scoped_ptr<ObjectStore> store(new FileStore(args[0], args[1]));
  g_ceph_context->_conf->filestore_update_collections = true;;
  g_ceph_context->_conf->debug_filestore = 20;
  int r = store->mount();
  if (r < 0)
    return -r;

  uint32_t version;
  r = store->version_stamp_is_valid(&version);
  if (r < 0)
    return -r;
  if (r == -1) {
    cerr << "FileStore is up to date." << std::endl;
    store->umount();
    return 0;
  } else {
    cerr << "FileStore is old at version " << version << ".  Updating..." 
	 << std::endl;
  }

  cerr << "Getting collections" << std::endl;
  vector<coll_t> collections;
  r = store->list_collections(collections);
  if (r < 0)
    return -r;

  cerr << collections.size() << " to process." << std::endl;
  int processed = 0;
  for (vector<coll_t>::iterator i = collections.begin();
       i != collections.end();
       ++i, ++processed) {
    cerr << processed << "/" << collections.size() << " processed" << std::endl;
    uint32_t collection_version;
    r = store->collection_version_current(*i, &collection_version);
    if (r < 0) {
      return r;
    } else if (r == 1) {
      cerr << "Collection " << *i << " is up to date" << std::endl;
    } else {
      cerr << "Updating collection " << *i << " current version is " << collection_version << std::endl;
      r = convert_collection(store.get(), *i);
      if (r < 0)
	return r;
      cerr << "collection " << *i << " updated" << std::endl;
    }
  }
  cerr << "All collections up to date, updating version stamp..." << std::endl;
  r = store->update_version_stamp();
  if (r < 0)
    return r;
  store->sync_and_flush();
  store->sync();
  cerr << "Version stamp updated, done!" << std::endl;
  store->umount();
  return 0;
}
