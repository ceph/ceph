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
  bool old_style_encode;

public:
  map<client_t,entity_inst_t> client_map;
  map<client_t,client_metadata_t> client_metadata_map;

  ESessions() : LogEvent(EVENT_SESSIONS), cmapv(0), old_style_encode(false) { }
  ESessions(version_t pv, map<client_t,entity_inst_t>&& cm,
	    map<client_t,client_metadata_t>&& cmm) :
    LogEvent(EVENT_SESSIONS),
    cmapv(pv), old_style_encode(false),
    client_map(std::move(cm)),
    client_metadata_map(std::move(cmm)) {}

  void mark_old_encoding() { old_style_encode = true; }

  void encode(bufferlist &bl, uint64_t features) const override;
  void decode_old(bufferlist::const_iterator &bl);
  void decode_new(bufferlist::const_iterator &bl);
  void decode(bufferlist::const_iterator &bl) override {
    if (old_style_encode) decode_old(bl);
    else decode_new(bl);
  }
  void dump(Formatter *f) const override;
  static void generate_test_instances(std::list<ESessions*>& ls);

  void print(ostream& out) const override {
    out << "ESessions " << client_map.size() << " opens cmapv " << cmapv;
  }
  
  void update_segment() override;
  void replay(MDSRank *mds) override;  
};
WRITE_CLASS_ENCODER_FEATURES(ESessions)

#endif
