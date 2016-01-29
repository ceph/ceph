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
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "os/filestore/FileStore.h"
#include "global/global_init.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout

struct Foo : public Thread {
  void *entry() {
    dout(0) << "foo started" << dendl;
    sleep(1);
    dout(0) << "foo asserting 0" << dendl;
    assert(0);
  }
} foo;

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  // args
  if (args.size() < 2) return -1;
  const char *filename = args[0];
  int mb = atoi(args[1]);

  cout << "#dev " << filename << std::endl;
  cout << "#mb " << mb << std::endl;

  ObjectStore *fs = new FileStore(filename, NULL);
  if (fs->mount() < 0) {
    cout << "mount failed" << std::endl;
    return -1;
  }

  ObjectStore::Sequencer osr(__func__);
  ObjectStore::Transaction t;
  char buf[1 << 20];
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  t.create_collection(coll_t(), 0);

  for (int i=0; i<mb; i++) {
    char f[30];
    snprintf(f, sizeof(f), "foo%d\n", i);
    sobject_t soid(f, CEPH_NOSNAP);
    t.write(coll_t(), ghobject_t(hobject_t(soid)), 0, bl.length(), bl);
  }
  
  dout(0) << "starting thread" << dendl;
  foo.create("foo");
  dout(0) << "starting op" << dendl;
  fs->apply_transaction(&osr, std::move(t));

}

