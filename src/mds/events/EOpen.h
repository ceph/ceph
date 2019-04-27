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

#ifndef CEPH_MDS_EOPEN_H
#define CEPH_MDS_EOPEN_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class EOpen : public LogEvent {
public:
  EMetaBlob metablob;
  vector<inodeno_t> inos;
  vector<vinodeno_t> snap_inos;

  EOpen() : LogEvent(EVENT_OPEN) { }
  explicit EOpen(MDLog *mdlog) :
    LogEvent(EVENT_OPEN) { }

  void print(ostream& out) const override {
    out << "EOpen " << metablob << ", " << inos.size() << " open files";
  }

  EMetaBlob *get_metablob() override { return &metablob; }

  void add_clean_inode(CInode *in) {
    if (!in->is_base()) {
      metablob.add_dir_context(in->get_projected_parent_dn()->get_dir());
      metablob.add_primary_dentry(in->get_projected_parent_dn(), 0, false);
      if (in->last == CEPH_NOSNAP)
	inos.push_back(in->ino());
      else
	snap_inos.push_back(in->vino());
    }
  }
  void add_ino(inodeno_t ino) {
    inos.push_back(ino);
  }

  void encode(bufferlist& bl, uint64_t features) const override;
  void decode(bufferlist::const_iterator& bl) override;
  void dump(Formatter *f) const override;
  static void generate_test_instances(std::list<EOpen*>& ls);

  void update_segment() override;
  void replay(MDSRank *mds) override;
};
WRITE_CLASS_ENCODER_FEATURES(EOpen)

#endif
