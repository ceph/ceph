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

#ifndef __MDS_EUPDATE_H
#define __MDS_EUPDATE_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class EUpdate : public LogEvent {
public:
  EMetaBlob metablob;
  string type;
  bufferlist client_map;

  EUpdate() : LogEvent(EVENT_UPDATE) { }
  EUpdate(MDLog *mdlog, const char *s) : 
    LogEvent(EVENT_UPDATE), metablob(mdlog),
    type(s) { }
  
  void print(ostream& out) {
    if (type.length())
      out << type << " ";
    out << metablob;
  }

  void encode_payload(bufferlist& bl) {
    ::_encode(type, bl);
    metablob._encode(bl);
    ::_encode(client_map, bl);
  } 
  void decode_payload(bufferlist& bl, int& off) {
    ::_decode(type, bl, off);
    metablob._decode(bl, off);
    ::_decode(client_map, bl, off);
  }

  void update_segment();
  void replay(MDS *mds);
};

#endif
