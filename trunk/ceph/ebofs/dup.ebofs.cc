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


int main(int argc, char **argv)
{
  vector<char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  // args
  if (args.size() != 2) {
    cerr << "usage: dup.ebofs src dst" << std::endl;
    return -1;
  }
  char *srcfn = args[0];
  char *dstfn = args[1];

  cout << "src " << srcfn << std::endl;
  cout << "dst " << dstfn << std::endl;

  Ebofs src(srcfn);
  Ebofs dst(dstfn);

  if (src.mount() < 0) return 1;
  if (dst.mkfs() < 0) return 1;
  if (dst.mount() < 0) return 1;

  // collections
  list<coll_t> collections;
  src.list_collections(collections);
  int num = collections.size();
  cout << num << " collections" << std::endl;
  int i = 1;
  for (list<coll_t>::iterator p = collections.begin();
       p != collections.end();
       ++p) {
    cout << "collection " << i++ << "/" << num << " " << hex << *p << dec << std::endl;
    dst.create_collection(*p, 0);
    map<string,bufferptr> attrs;
    src.collection_getattrs(*p, attrs);
    dst.collection_setattrs(*p, attrs, 0);
  }

  // objects
  list<object_t> objects;
  src.list_objects(objects);
  num = objects.size();
  cout << num << " objects" << std::endl;
  i = 1;
  for (list<object_t>::iterator p = objects.begin(); p != objects.end(); ++p) {
    bufferlist bl;
    src.read(*p, 0, 0, bl);
    cout << "object " << i++ << "/" << num << " " << *p << " = " << bl.length() << " bytes" << std::endl;
    dst.write(*p, 0, bl.length(), bl, 0);
    map<string,bufferptr> attrs;
    src.getattrs(*p, attrs);
    dst.setattrs(*p, attrs);
    set<coll_t> c;
    src.get_object_collections(*p, c);
    for (set<coll_t>::iterator q = c.begin(); q != c.end(); q++) 
      dst.collection_add(*q, *p, 0);
  }
  
  src.umount();
  dst.umount();  
  return 0;
}
