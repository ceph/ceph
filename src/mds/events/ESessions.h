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

#ifndef CEPH_MDS_ESESSIONS_H
#define CEPH_MDS_ESESSIONS_H

#include "common/config.h"
#include "include/types.h"

#include "../LogEvent.h"

class ESessions : public LogEvent {
protected:
  version_t cmapv;  // client map version

public:
  map<client_t,entity_inst_t> client_map;
  bool old_style_encode;

  ESessions() : LogEvent(EVENT_SESSIONS), cmapv(0), old_style_encode(false) { }
  ESessions(version_t pv, map<client_t,entity_inst_t>& cm) :
    LogEvent(EVENT_SESSIONS),
    cmapv(pv),
    old_style_encode(false) {
    client_map.swap(cm);
  }

  void mark_old_encoding() { old_style_encode = true; }

  void encode(bufferlist &bl, uint64_t features) const;
  void decode_old(bufferlist::iterator &bl);
  void decode_new(bufferlist::iterator &bl);
  void decode(bufferlist::iterator &bl) {
    if (old_style_encode) decode_old(bl);
    else decode_new(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ESessions*>& ls);

  void print(ostream& out) const {
    out << "ESessions " << client_map.size() << " opens cmapv " << cmapv;
  }
  
  void update_segment();
  void replay(MDSRank *mds);  
};
WRITE_CLASS_ENCODER_FEATURES(ESessions)

#endif
