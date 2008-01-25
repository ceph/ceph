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

#define dout(x) if (x <= g_conf.debug_ebofs) *_dout << dbeginl

#include <iostream>
#include "ebofs/Ebofs.h"

map<off_t, pair<utime_t,utime_t> > writes;

Mutex lock;

struct C_Commit : public Context {
  off_t off;
  C_Commit(off_t o) : off(o) {}
  void finish(int r) {
    Mutex::Locker l(lock);
    utime_t now = g_clock.now();
    dout(0) << off << "\t" 
	 << (writes[off].second-writes[off].first) << "\t"
	 << (now - writes[off].first) << dendl;
    writes.erase(off);
  }
};


int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

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
       << seconds << " seconds, " << bytes << " bytes per write" << std::endl;

  Ebofs fs(filename, journal);
  if (fs.mkfs() < 0) {
    cout << "mkfs failed" << std::endl;
    return -1;
  }
  if (fs.mount() < 0) {
    cout << "mount failed" << std::endl;
    return -1;
  }

  utime_t now = g_clock.now();
  utime_t end = now;
  end += seconds;
  off_t pos = 0;
  //cout << "stop at " << end << std::endl;
  cout << "# offset\tack\tcommit" << std::endl;
  while (now < end) {
    object_t oid(1,1);
    utime_t start;
    {
      Mutex::Locker l(lock);
      start = writes[pos].first = now;
    }
    fs.write(oid, pos, bytes, bl, new C_Commit(pos));
    now = g_clock.now();
    {
      Mutex::Locker l(lock);
      writes[pos].second = now;
    }
    pos += bytes;

    // wait?
    utime_t next = start;
    next += interval;
    if (now < next) {
      float s = next - now;
      s *= 1000 * 1000;  // s -> us
      //cout << "sleeping for " << s << std::endl;
      usleep(s);
    }
  }

  fs.umount();

}

