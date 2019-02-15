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

#include <string_view>

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
  EUpdate(MDLog *mdlog, std::string_view s) :
    LogEvent(EVENT_UPDATE),
    type(s), cmapv(0), had_slaves(false) { }
  
  void print(ostream& out) const override {
    if (type.length())
      out << "EUpdate " << type << " ";
    out << metablob;
  }

  EMetaBlob *get_metablob() override { return &metablob; }

  void encode(bufferlist& bl, uint64_t features) const override;
  void decode(bufferlist::const_iterator& bl) override;
  void dump(Formatter *f) const override;
  static void generate_test_instances(list<EUpdate*>& ls);

  void update_segment() override;
  void replay(MDSRank *mds) override;
};
WRITE_CLASS_ENCODER_FEATURES(EUpdate)

#endif
