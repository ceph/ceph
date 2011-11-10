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
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"

#undef dout_prefix
#define dout_prefix *_dout

void usage()
{
  cerr << "usage: [write,verify] dir journal" << std::endl;
  exit(1);
}

hobject_t get_clone_name(uint64_t x)
{
  char n[40];
  sprintf(n, "clone-%llu", (unsigned long long)x);
  return hobject_t(sobject_t(n, 0));
}

uint64_t committed = 0;

struct C_Ondisk : public Context {
  uint64_t x;
  C_Ondisk(uint64_t t) : x(t) {}
  void finish(int r) {
    committed = x;
  }
};

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  // args
  if (args.size() < 3)
    usage();
  string mode = args[0];
  if (mode != "write" && mode != "verify")
    usage();
  const char *filename = args[1];
  const char *journal = args[2];

  FileStore *fs = new FileStore(filename, journal);

  coll_t coll("foo");
  sobject_t soid("obj", 0);
  hobject_t oid(soid);

  if (mode == "write") {
    if (fs->mkfs() < 0) {
      cerr << "mkfs failed" << std::endl;
      return -1;
    }
    if (fs->mount() < 0) {
      cerr << "mount failed" << std::endl;
      return -1;
    }

    ObjectStore::Transaction ft;
    ft.create_collection(coll);
    fs->apply_transaction(ft);

    uint64_t size = 4096;
    uint64_t x = 1;

    bool committing = false;
    uint64_t committed_at = 0;
    while (true) {
      // build buffer
      bufferlist bl;
      while (bl.length() < size)
	::encode(x, bl);

      hobject_t clone = get_clone_name(x - 1);
      
      ObjectStore::Transaction t;
      t.clone(coll, oid, clone);
      fs->apply_transaction(t, new C_Ondisk(x));

      ObjectStore::Transaction t2;
      t2.write(coll, oid, 0, bl.length(), bl);
      fs->apply_transaction(t2, new C_Ondisk(x));

      ++x;
      cout << x << "\n";

      if (fs->is_committing()) {
	committing = true;
      } else if (committing) {
	committing = false;
	if (!committed_at) {
	  committed_at = x;
	  cout << " saw a journal flush complete at around " << committed_at << std::endl;
	}
      }
      if (committed_at && committed > committed_at + 100) {
	cout << " have seen several commits since the last journal flush, exiting to fake a crash" << std::endl;
	_exit(0);
      }
    }
  }

  if (mode == "verify") {
    if (fs->mount() < 0) {
      cerr << "mount failed" << std::endl;
      return -1;
    }

    bufferlist bl;
    int r = fs->read(coll, oid, 0, 0, bl);
    assert(r >= 0);
    uint64_t max;
    bufferlist::iterator p = bl.begin();
    ::decode(max, p);
    
    cout << "max = " << max << std::endl;
    int errors = 0;
    for (uint64_t x = 1; x < max; ++x) {
      hobject_t clone = get_clone_name(x);
      
      bufferlist bl;
      fs->read(coll, clone, 0, 0, bl);
      bufferlist::iterator p = bl.begin();
      uint64_t t;
      ::decode(t, p);
      
      if (t != x) {
	cerr << clone << " contains " << t << " instead of " << x << std::endl;
	errors++;
      }
    }
    cout << errors << " errors" << std::endl;
    fs->umount();
    return (errors > 0);
  }
  return 1;
}

