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
//#include "ebofs/Ebofs.h"
#include "os/FileStore.h"
#include "common/common_init.h"

#undef dout_prefix
#define dout_prefix *_dout << dbeginl

struct io {
  utime_t start, ack, commit;
  bool done() {
    return ack.sec() && commit.sec();
  }
};
map<off_t,io> writes;

Mutex lock("streamtest.cc lock");


void pr(off_t off)
{
  io &i = writes[off];
  dout(2) << off << "\t" 
	  << (i.ack - i.start) << "\t"
	  << (i.commit - i.start) << dendl;
  writes.erase(off);
}

void set_start(off_t off, utime_t t)
{
  Mutex::Locker l(lock);
  writes[off].start = t;
}

void set_ack(off_t off, utime_t t)
{
  Mutex::Locker l(lock);
  writes[off].ack = t;
  if (writes[off].done())
    pr(off);
}

void set_commit(off_t off, utime_t t)
{
  Mutex::Locker l(lock);
  writes[off].commit = t;
  if (writes[off].done())
    pr(off);
}


struct C_Commit : public Context {
  off_t off;
  C_Commit(off_t o) : off(o) {}
  void finish(int r) {
    set_commit(off, g_clock.now());
  }
};


int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  common_init(args, NULL, false);

  // args
  if (args.size() < 3) return -1;
  const char *filename = args[0];
  int seconds = atoi(args[1]);
  int bytes = atoi(args[2]);
  const char *journal = 0;
  if (args.size() >= 4)
    journal = args[3];

  buffer::ptr bp(bytes);
  bp.zero();
  bufferlist bl;
  bl.push_back(bp);

  float interval = 1.0 / 1000;
  
  cout << "#dev " << filename
       << ", " << seconds << " seconds, " << bytes << " bytes per write" << std::endl;

  //ObjectStore *fs = new Ebofs(filename, journal);
  ObjectStore *fs = new FileStore(filename);

  if (fs->mount() < 0) {
    cout << "mount failed" << std::endl;
    return -1;
  }

  ObjectStore::Transaction ft;
  ft.create_collection(0);
  fs->apply_transaction(ft);

  utime_t now = g_clock.now();
  utime_t end = now;
  end += seconds;
  off_t pos = 0;
  //cout << "stop at " << end << std::endl;
  cout << "# offset\tack\tcommit" << std::endl;
  while (now < end) {
    pobject_t poid(0, 0, object_t(1, 1));
    utime_t start = now;
    set_start(pos, now);
    ObjectStore::Transaction t;
    t.write(0, poid, pos, bytes, bl);
    fs->apply_transaction(t, new C_Commit(pos));
    now = g_clock.now();
    set_ack(pos, now);
    pos += bytes;

    // wait?
    utime_t next = start;
    next += interval;
    if (now < next) {
      float s = next - now;
      s *= 1000 * 1000;  // s -> us
      //cout << "sleeping for " << s << " us" << std::endl;
      usleep((int)s);
    }
  }

  fs->umount();

}

