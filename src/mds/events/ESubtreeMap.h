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
#include "../SegmentBoundary.h"
#include "EMetaBlob.h"

class ESubtreeMap : public LogEvent, public SegmentBoundary {
public:
  EMetaBlob metablob;
  std::map<dirfrag_t, std::vector<dirfrag_t> > subtrees;
  std::set<dirfrag_t> ambiguous_subtrees;
  uint64_t expire_pos = 0;

  ESubtreeMap() : LogEvent(EVENT_SUBTREEMAP) {}
  
  void print(std::ostream& out) const override {
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
  bool is_major_segment_boundary() const override {
    return true;
  }
};
WRITE_CLASS_ENCODER_FEATURES(ESubtreeMap)

#endif
