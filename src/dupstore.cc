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

#include <ext/hash_map>
using __gnu_cxx::hash_map;

int dupstore(ObjectStore* src, ObjectStore* dst)
{
  if (src->mount() < 0) return 1;
  if (dst->mkfs() < 0) return 1;
  if (dst->mount() < 0) return 1;

  // objects
  hash_map<hobject_t, coll_t> did_object;

  // collections
  vector<coll_t> collections;
  src->list_collections(collections);
  int num = collections.size();
  cout << num << " collections" << std::endl;
  int i = 1;
  for (vector<coll_t>::iterator p = collections.begin();
       p != collections.end();
       ++p) {
    cout << "collection " << i++ << "/" << num << " " << hex << *p << dec << std::endl;
    {
      ObjectStore::Transaction t;
      t.create_collection(*p);
      map<string,bufferptr> attrs;
      src->collection_getattrs(*p, attrs);
      t.collection_setattrs(*p, attrs);
      dst->apply_transaction(t);
    }

    vector<hobject_t> o;
    src->collection_list(*p, o);
    int numo = o.size();
    int j = 1;
    for (vector<hobject_t>::iterator q = o.begin(); q != o.end(); q++) {
      ObjectStore::Transaction t;
      if (did_object.count(*q))
	t.collection_add(*p, did_object[*q], *q);
      else {
	bufferlist bl;
	src->read(*p, *q, 0, 0, bl);
	cout << "object " << j++ << "/" << numo << " " << *q << " = " << bl.length() << " bytes" << std::endl;
	t.write(*p, *q, 0, bl.length(), bl);
	map<string,bufferptr> attrs;
	src->getattrs(*p, *q, attrs);
	t.setattrs(*p, *q, attrs);
	did_object[*q] = *p;
      }
      dst->apply_transaction(t);
    }
  }
  
  src->umount();
  dst->umount();  
  return 0;
}

void usage()
{
  cerr << "usage: ceph_dupstore filestore SRC filestore DST" << std::endl;
  exit(0);
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  // args
  if (args.size() != 4) 
    usage();

  ObjectStore *src = 0, *dst = 0;

  if (strcmp(args[0], "filestore") == 0) 
    src = new FileStore(args[1], NULL);
  else usage();

  if (strcmp(args[2], "filestore") == 0) 
    dst = new FileStore(args[3], NULL);
  else usage();

  return dupstore(src, dst);
}
