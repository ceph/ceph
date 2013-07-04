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

  EOpen() : LogEvent(EVENT_OPEN) { }
  EOpen(MDLog *mdlog) : 
    LogEvent(EVENT_OPEN), metablob(mdlog) { }

  void print(ostream& out) const {
    out << "EOpen " << metablob << ", " << inos.size() << " open files";
  }

  void add_clean_inode(CInode *in) {
    if (!in->is_base()) {
      metablob.add_dir_context(in->get_projected_parent_dn()->get_dir());
      metablob.add_primary_dentry(in->get_projected_parent_dn(), 0, false);
      inos.push_back(in->ino());
    }
  }
  void add_ino(inodeno_t ino) {
    inos.push_back(ino);
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<EOpen*>& ls);

  void update_segment();
  void replay(MDS *mds);
};

#endif
