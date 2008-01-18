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

#ifndef __MDS_ESESSION_H
#define __MDS_ESESSION_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "../LogEvent.h"

class ESession : public LogEvent {
 protected:
  entity_inst_t client_inst;
  bool open;    // open or close
  version_t cmapv;  // client map version

 public:
  ESession() : LogEvent(EVENT_SESSION) { }
  ESession(entity_inst_t inst, bool o, version_t v) :
    LogEvent(EVENT_SESSION),
    client_inst(inst),
    open(o),
    cmapv(v) {
  }
  
  void encode_payload(bufferlist& bl) {
    ::_encode(client_inst, bl);
    ::_encode(open, bl);
    ::_encode(cmapv, bl);
  }
  void decode_payload(bufferlist& bl, int& off) {
    ::_decode(client_inst, bl, off);
    ::_decode(open, bl, off);
    ::_decode(cmapv, bl, off);
  }


  void print(ostream& out) {
    if (open)
      out << "ESession " << client_inst << " open cmapv " << cmapv;
    else
      out << "ESession " << client_inst << " close cmapv " << cmapv;
  }
  
  void update_segment();
  void replay(MDS *mds);  
};

#endif
