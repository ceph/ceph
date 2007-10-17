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

#ifndef __MONMAP_H
#define __MONMAP_H

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "msg/Message.h"
#include "include/types.h"

class MonMap {
 public:
  epoch_t epoch;       // what epoch/version of the monmap
  int32_t num_mon;
  vector<entity_inst_t> mon_inst;

  int       last_mon;    // last mon i talked to

  MonMap(int s=0) : epoch(0), num_mon(s), mon_inst(s), last_mon(-1) {}

  void add_mon(entity_inst_t inst) {
    mon_inst.push_back(inst);
    num_mon++;
  }

  // pick a mon.  
  // choice should be stable, unless we explicitly ask for a new one.
  int pick_mon(bool newmon=false) { 
    if (newmon || (last_mon < 0)) {
      last_mon = rand() % num_mon;
    }
    return last_mon;    
  }

  const entity_inst_t &get_inst(int m) {
    assert(m < num_mon);
    return mon_inst[m];
  }

  void encode(bufferlist& blist) {
    ::_encode(epoch, blist);
    ::_encode(num_mon, blist);
    ::_encode(mon_inst, blist);
  }
  
  void decode(bufferlist& blist) {
    int off = 0;
    ::_decode(epoch, blist, off);
    ::_decode(num_mon, blist, off);
    ::_decode(mon_inst, blist, off);
  }

  // read from/write to a file
  int write(char *fn) {
    // encode
    bufferlist bl;
    encode(bl);

    // write
    int fd = ::open(fn, O_RDWR|O_CREAT);
    if (fd < 0) return fd;
    ::fchmod(fd, 0644);
    ::write(fd, (void*)bl.c_str(), bl.length());
    ::close(fd);
    return 0;
  }

  int read(char *fn) {
    // read
    bufferlist bl;
    int fd = ::open(fn, O_RDONLY);
    if (fd < 0) return fd;
    struct stat st;
    ::fstat(fd, &st);
    bufferptr bp(st.st_size);
    bl.append(bp);
    ::read(fd, (void*)bl.c_str(), bl.length());
    ::close(fd);
  
    // decode
    decode(bl);
    return 0;
  }

};

#endif
