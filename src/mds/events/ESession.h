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

#ifndef CEPH_MDS_ESESSION_H
#define CEPH_MDS_ESESSION_H

#include "common/config.h"
#include "include/types.h"

#include "../LogEvent.h"

class ESession : public LogEvent {
 protected:
  entity_inst_t client_inst;
  bool open;    // open or close
  version_t cmapv;  // client map version

  interval_set<inodeno_t> inos;
  version_t inotablev;

 public:
  ESession() : LogEvent(EVENT_SESSION) { }
  ESession(entity_inst_t inst, bool o, version_t v) :
    LogEvent(EVENT_SESSION),
    client_inst(inst),
    open(o),
    cmapv(v),
    inotablev(0) {
  }
  ESession(entity_inst_t inst, bool o, version_t v,
	   const interval_set<inodeno_t>& i, version_t iv) :
    LogEvent(EVENT_SESSION),
    client_inst(inst),
    open(o),
    cmapv(v),
    inos(i), inotablev(iv) { }

  void encode(bufferlist &bl) const {
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    ::encode(stamp, bl);
    ::encode(client_inst, bl);
    ::encode(open, bl);
    ::encode(cmapv, bl);
    ::encode(inos, bl);
    ::encode(inotablev, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    if (struct_v >= 2)
      ::decode(stamp, bl);
    ::decode(client_inst, bl);
    ::decode(open, bl);
    ::decode(cmapv, bl);
    ::decode(inos, bl);
    ::decode(inotablev, bl);
  }


  void print(ostream& out) {
    if (open)
      out << "ESession " << client_inst << " open cmapv " << cmapv;
    else
      out << "ESession " << client_inst << " close cmapv " << cmapv;
    if (inos.size())
      out << " (" << inos.size() << " inos, v" << inotablev << ")";
  }
  
  void update_segment();
  void replay(MDS *mds);  
};

#endif
