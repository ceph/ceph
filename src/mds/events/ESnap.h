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

#ifndef __MDS_ESNAP_H
#define __MDS_ESNAP_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "../LogEvent.h"
#include "../snap.h"

class ESnap : public LogEvent {
public:
  bool create;   
  SnapInfo snap;
  version_t version;    // table version

 public:
  ESnap() : LogEvent(EVENT_SNAP) { }
  ESnap(bool c, SnapInfo &sn, version_t v) : 
    LogEvent(EVENT_SNAP),
    create(c), snap(sn), version(v) { }

  void encode(bufferlist& bl) const {
    ::encode(create, bl);
    ::encode(snap, bl);
    ::encode(version, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(create, bl);
    ::decode(snap, bl);
    ::decode(version, bl);
  }

  void print(ostream& out) {
    out << "ESnap " << (create ? "create":"remove")
	<< " " << snap 
	<< " v " << version;
  }  

  void update_segment();
  void replay(MDS *mds);  
};

#endif
