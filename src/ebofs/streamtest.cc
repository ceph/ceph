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

map<off_t, pair<utime_t,utime_t> > writes;

struct C_Commit : public Context {
  off_t off;
  C_Commit(off_t o) : off(o) {}
  void finish(int r) {
    utime_t now = g_clock.now();
    cout << off << "\t" 
	 << (writes[off].second-writes[off].first) << "\t"
	 << (now - writes[off].first) << std::endl;
    writes.erase(off);
  }
};


int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  // args
  if (args.size() != 3) return -1;
  const char *filename = args[0];
  int seconds = atoi(args[1]);
  int bytes = atoi(args[2]);

  buffer::ptr bp(bytes);
  bp.zero();
  bufferlist bl;
  bl.push_back(bp);
  

  cout << "#dev " << filename
       << seconds << " seconds, " << bytes << " bytes per write" << std::endl;

  Ebofs fs(filename);
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
    writes[pos].first = now;
    fs.write(oid, pos, bytes, bl, new C_Commit(pos));
    now = g_clock.now();
    writes[pos].second = now;
    pos += bytes;
  }

  fs.umount();

}

