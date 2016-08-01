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

#ifndef CEPH_MDS_EUPDATE_H
#define CEPH_MDS_EUPDATE_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class EUpdate : public LogEvent {
public:
  EMetaBlob metablob;
  string type;
  bufferlist client_map;
  version_t cmapv;
  metareqid_t reqid;
  bool had_slaves;

  EUpdate() : LogEvent(EVENT_UPDATE), cmapv(0), had_slaves(false) { }
  EUpdate(MDLog *mdlog, const char *s) : 
    LogEvent(EVENT_UPDATE), metablob(mdlog),
    type(s), cmapv(0), had_slaves(false) { }
  
  void print(ostream& out) const {
    if (type.length())
      out << "EUpdate " << type << " ";
    out << metablob;
  }

  EMetaBlob *get_metablob() { return &metablob; }

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<EUpdate*>& ls);

  void update_segment();
  void replay(MDSRank *mds);
  EMetaBlob const *get_metablob() const {return &metablob;}
};
WRITE_CLASS_ENCODER_FEATURES(EUpdate)

#endif
