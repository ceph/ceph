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

#ifndef __MREMOVESNAPS_H
#define __MREMOVESNAPS_H

#include "msg/Message.h"

struct MRemoveSnaps : public Message {
  map<int, vector<snapid_t> > snaps;
  
  MRemoveSnaps() : 
    Message(MSG_REMOVE_SNAPS) { }
  MRemoveSnaps(map<int, vector<snapid_t> >& s) : 
    Message(MSG_REMOVE_SNAPS) {
    snaps.swap(s);
  }
  
  const char *get_type_name() { return "remove_snaps"; }
  void print(ostream& out) {
    out << "remove_snaps(" << snaps << ")";
  }

  void encode_payload() {
    ::encode(snaps, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(snaps, p);
    assert(p.end());
  }

};

#endif
