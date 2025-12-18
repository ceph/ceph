// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#ifndef CEPH_EIMPORTSTART_H
#define CEPH_EIMPORTSTART_H

#include "common/config.h"
#include "include/types.h"

class MDLog;
class MDSRank;

#include "EMetaBlob.h"
#include "../LogEvent.h"

class EImportStart : public LogEvent {
protected:
  dirfrag_t base;
  std::vector<dirfrag_t> bounds;
  mds_rank_t from;

public:
  EMetaBlob metablob;
  bufferlist client_map;  // encoded map<__u32,entity_inst_t>
  version_t cmapv{0};

  EImportStart(MDLog *log, dirfrag_t di, const std::vector<dirfrag_t>& b, mds_rank_t f) :
    LogEvent(EVENT_IMPORTSTART),
    base(di), bounds(b), from(f) { }
  EImportStart() :
    LogEvent(EVENT_IMPORTSTART), from(MDS_RANK_NONE) { }
  
  void print(std::ostream& out) const override {
    out << "EImportStart " << base << " from mds." << from << " " << metablob;
  }

  EMetaBlob *get_metablob() override { return &metablob; }
  
  void encode(bufferlist &bl, uint64_t features) const override;
  void decode(bufferlist::const_iterator &bl) override;
  void dump(Formatter *f) const override;
  static std::list<EImportStart> generate_test_instances();
  
  void update_segment() override;
  void replay(MDSRank *mds) override;

};
WRITE_CLASS_ENCODER_FEATURES(EImportStart)

#endif
