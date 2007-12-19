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
#include "ebofs/Ebofs.h"
#include "osd/FakeStore.h"


int dupstore(ObjectStore* src, ObjectStore* dst)
{
  if (src->mount() < 0) return 1;
  if (dst->mkfs() < 0) return 1;
  if (dst->mount() < 0) return 1;

  // objects
  list<pobject_t> objects;
  src->list_objects(objects);
  int num = objects.size();
  cout << num << " objects" << std::endl;
  int i = 1;
  for (list<pobject_t>::iterator p = objects.begin(); p != objects.end(); ++p) {
    bufferlist bl;
    src->read(*p, 0, 0, bl);
    cout << "object " << i++ << "/" << num << " " << *p << " = " << bl.length() << " bytes" << std::endl;
    dst->write(*p, 0, bl.length(), bl, 0);
    map<string,bufferptr> attrs;
    src->getattrs(*p, attrs);
    dst->setattrs(*p, attrs);
  }

  // collections
  list<coll_t> collections;
  src->list_collections(collections);
  num = collections.size();
  cout << num << " collections" << std::endl;
  i = 1;
  for (list<coll_t>::iterator p = collections.begin();
       p != collections.end();
       ++p) {
    dst->create_collection(*p, 0);
    map<string,bufferptr> attrs;
    src->collection_getattrs(*p, attrs);
    dst->collection_setattrs(*p, attrs);
    list<pobject_t> o;
    src->collection_list(*p, o);
    int numo = 0;
    for (list<pobject_t>::iterator q = o.begin(); q != o.end(); q++) {
      dst->collection_add(*p, *q, 0);
      numo++;
    }
    cout << "collection " << i++ << "/" << num << " " << hex << *p << dec << " = " << numo << " objects" << std::endl;
  }

  
  src->umount();
  dst->umount();  
  return 0;
}

void usage()
{
  cerr << "usage: dup.ebofs (ebofs|fakestore) src (ebofs|fakestore) dst" << std::endl;
  exit(0);
}

int main(int argc, char **argv)
{
  vector<char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  // args
  if (args.size() != 4) 
    usage();

  ObjectStore *src, *dst;

  if (strcmp(args[0], "ebofs") == 0) 
    src = new Ebofs(args[1]);
  else if (strcmp(args[0], "fakestore") == 0) 
    src = new FakeStore(args[1]);
  else usage();

  if (strcmp(args[2], "ebofs") == 0) 
    dst = new Ebofs(args[3]);
  else if (strcmp(args[2], "fakestore") == 0) 
    dst = new FakeStore(args[3]);
  else usage();

  return dupstore(src, dst);
}
