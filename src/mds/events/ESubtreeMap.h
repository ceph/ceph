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

#ifndef CEPH_MDS_ESUBTREEMAP_H
#define CEPH_MDS_ESUBTREEMAP_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class ESubtreeMap : public LogEvent {
public:
  EMetaBlob metablob;
  map<dirfrag_t, vector<dirfrag_t> > subtrees;
  set<dirfrag_t> ambiguous_subtrees;
  uint64_t expire_pos;
  uint64_t event_seq;

  ESubtreeMap() : LogEvent(EVENT_SUBTREEMAP), expire_pos(0), event_seq(0) { }
  
  void print(ostream& out) const override {
    out << "ESubtreeMap " << subtrees.size() << " subtrees " 
	<< ", " << ambiguous_subtrees.size() << " ambiguous "
	<< metablob;
  }

  EMetaBlob *get_metablob() override { return &metablob; }

  void encode(bufferlist& bl, uint64_t features) const override;
  void decode(bufferlist::const_iterator& bl) override;
  void dump(Formatter *f) const override;
  static void generate_test_instances(std::list<ESubtreeMap*>& ls);

  void replay(MDSRank *mds) override;
};
WRITE_CLASS_ENCODER_FEATURES(ESubtreeMap)

#endif
