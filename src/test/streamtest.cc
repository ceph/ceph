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

struct io {
  utime_t start, ack, commit;
  bool done() {
    return ack.sec() && commit.sec();
  }
};
map<off_t,io> writes;
Cond cond;
Mutex test_lock("streamtest.cc lock");

unsigned concurrent = 1;
void throttle()
{ 
  Mutex::Locker l(test_lock);
  while (writes.size() >= concurrent) {
    //generic_dout(0) << "waiting" << dendl;
    cond.Wait(test_lock);
  }
}

double total_ack = 0;
double total_commit = 0;
int total_num = 0;

void pr(off_t off)
{
  io &i = writes[off];
  if (false) cout << off << "\t" 
       << (i.ack - i.start) << "\t"
       << (i.commit - i.start) << std::endl;
  total_num++;
  total_ack += (i.ack - i.start);
  total_commit += (i.commit - i.start);
  writes.erase(off);
  cond.Signal();
}

void set_start(off_t off, utime_t t)
{
  Mutex::Locker l(test_lock);
  writes[off].start = t;
}

void set_ack(off_t off, utime_t t)
{
  Mutex::Locker l(test_lock);
  //generic_dout(0) << "ack " << off << dendl;
  writes[off].ack = t;
  if (writes[off].done())
    pr(off);
}

void set_commit(off_t off, utime_t t)
{
  Mutex::Locker l(test_lock);
  //generic_dout(0) << "commit " << off << dendl;
  writes[off].commit = t;
  if (writes[off].done())
    pr(off);
}


struct C_Ack : public Context {
  off_t off;
  C_Ack(off_t o) : off(o) {}
  void finish(int r) {
    set_ack(off, ceph_clock_now(g_ceph_context));
  }
};
struct C_Commit : public Context {
  off_t off;
  C_Commit(off_t o) : off(o) {}
  void finish(int r) {
    set_commit(off, ceph_clock_now(g_ceph_context));
  }
};


int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  // args
  if (args.size() < 3) return -1;
  const char *filename = args[0];
  int seconds = atoi(args[1]);
  int bytes = atoi(args[2]);
  const char *journal = 0;
  if (args.size() >= 4)
    journal = args[3];
  if (args.size() >= 5)
    concurrent = atoi(args[4]);

  cout << "concurrent = " << concurrent << std::endl;

  buffer::ptr bp(bytes);
  bp.zero();
  bufferlist bl;
  bl.push_back(bp);

  //float interval = 1.0 / 1000;
  
  cout << "#dev " << filename
       << ", " << seconds << " seconds, " << bytes << " bytes per write" << std::endl;

  ObjectStore *fs = new FileStore(filename, journal);
  
  if (fs->mkfs() < 0) {
    cout << "mkfs failed" << std::endl;
    return -1;
  }
  
  if (fs->mount() < 0) {
    cout << "mount failed" << std::endl;
    return -1;
  }

  ObjectStore::Sequencer osr(__func__);
  ObjectStore::Transaction ft;
  ft.create_collection(coll_t(), 0);
  fs->apply_transaction(&osr, ft);

  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t start = now;
  utime_t end = now;
  end += seconds;
  off_t pos = 0;
  //cout << "stop at " << end << std::endl;
  cout << "# offset\tack\tcommit" << std::endl;
  while (now < end) {
    sobject_t poid(object_t("streamtest"), 0);

    set_start(pos, ceph_clock_now(g_ceph_context));
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->write(coll_t(), ghobject_t(hobject_t(poid)), pos, bytes, bl);
    fs->queue_transaction(NULL, t, new C_Ack(pos), new C_Commit(pos));
    pos += bytes;

    throttle();

    now = ceph_clock_now(g_ceph_context);

    // wait?
    /*
    utime_t next = start;
    next += interval;
    if (now < next) {
      float s = next - now;
      s *= 1000 * 1000;  // s -> us
      //cout << "sleeping for " << s << " us" << std::endl;
      usleep((int)s);
    }
    */
  }

  cout << "total num " << total_num << std::endl;
  cout << "avg ack\t" << (total_ack / (double)total_num) << std::endl;
  cout << "avg commit\t" << (total_commit / (double)total_num) << std::endl;
  cout << "tput\t" << prettybyte_t((double)(total_num * bytes) / (double)(end-start)) << "/sec" << std::endl;

  fs->umount();

}

