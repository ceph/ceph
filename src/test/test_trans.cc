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
#include "os/bluestore/BlueStore.h"
#include "global/global_init.h"
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout

using namespace std;

struct Foo : public Thread {
  void *entry() override {
    dout(0) << "foo started" << dendl;
    sleep(1);
    dout(0) << "foo asserting 0" << dendl;
    ceph_abort();
  }
} foo;

int main(int argc, const char **argv)
{
  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  // args
  if (args.size() < 2) return -1;
  const char *filename = args[0];
  int mb = atoi(args[1]);

  cout << "#dev " << filename << std::endl;
  cout << "#mb " << mb << std::endl;

  ObjectStore *fs = new BlueStore(cct.get(), filename);
  if (fs->mount() < 0) {
    cout << "mount failed" << std::endl;
    return -1;
  }

  ObjectStore::Transaction t;
  char buf[1 << 20];
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  auto ch = fs->create_new_collection(coll_t());
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
  fs->queue_transaction(ch, std::move(t));
}

