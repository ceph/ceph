// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#ifndef __MDS_EUPDATE_H
#define __MDS_EUPDATE_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class EUpdate : public LogEvent {
public:
  EMetaBlob metablob;
  string type;

  EUpdate() : LogEvent(EVENT_UPDATE) { }
  EUpdate(const char *s) : LogEvent(EVENT_UPDATE),
			   type(s) { }
  
  void print(ostream& out) {
    if (type.length())
      out << type << " ";
    out << metablob;
  }

  void encode_payload(bufferlist& bl) {
    ::_encode(type, bl);
    metablob._encode(bl);
  } 
  void decode_payload(bufferlist& bl, int& off) {
    ::_decode(type, bl, off);
    metablob._decode(bl, off);
  }

  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);
};

#endif
