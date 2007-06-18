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

#ifndef __MDS_ECLIENTMAP_H
#define __MDS_ECLIENTMAP_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "../LogEvent.h"

class EClientMap : public LogEvent {
 protected:
  bufferlist mapbl;
  version_t cmapv;  // client map version

 public:
  EClientMap() : LogEvent(EVENT_CLIENTMAP) { }
  EClientMap(bufferlist& bl, version_t v) :
    LogEvent(EVENT_CLIENTMAP),
    cmapv(v) {
    mapbl.claim(bl);
  }
  
  void encode_payload(bufferlist& bl) {
    bl.append((char*)&cmapv, sizeof(cmapv));
    ::_encode(mapbl, bl);
  }
  void decode_payload(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(cmapv), (char*)&cmapv);
    off += sizeof(cmapv);
    ::_decode(mapbl, bl, off);
  }


  void print(ostream& out) {
    out << "EClientMap v " << cmapv;
  }
  

  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);
  
};

#endif
