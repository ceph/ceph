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
  metareqid_t reqid;
  bool had_slaves;

  EUpdate() : LogEvent(EVENT_UPDATE) { }
  EUpdate(MDLog *mdlog, const char *s) : 
    LogEvent(EVENT_UPDATE), metablob(mdlog),
    type(s), had_slaves(false) { }
  
  void print(ostream& out) {
    if (type.length())
      out << "EUpdate " << type << " ";
    out << metablob;
  }

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(type, bl);
    ::encode(metablob, bl);
    ::encode(client_map, bl);
    ::encode(reqid, bl);
    ::encode(had_slaves, bl);
  } 
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(type, bl);
    ::decode(metablob, bl);
    ::decode(client_map, bl);
    ::decode(reqid, bl);
    ::decode(had_slaves, bl);
  }

  void update_segment();
  void replay(MDS *mds);
};

#endif
