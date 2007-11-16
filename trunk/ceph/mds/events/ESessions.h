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

#ifndef __MDS_ESESSIONS_H
#define __MDS_ESESSIONS_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "../LogEvent.h"

class ESessions : public LogEvent {
protected:
  version_t cmapv;  // client map version

public:
  map<int,entity_inst_t> client_map;

  ESessions() : LogEvent(EVENT_SESSION) { }
  ESessions(version_t v) :
    LogEvent(EVENT_SESSION),
    cmapv(v) {
  }
  
  void encode_payload(bufferlist& bl) {
    ::_encode(client_map, bl);
    ::_encode(cmapv, bl);
  }
  void decode_payload(bufferlist& bl, int& off) {
    ::_decode(client_map, bl, off);
    ::_decode(cmapv, bl, off);
  }


  void print(ostream& out) {
    out << "ESessions " << client_map.size() << " opens cmapv " << cmapv;
  }
  
  void update_segment();
  void replay(MDS *mds);  
};

#endif
