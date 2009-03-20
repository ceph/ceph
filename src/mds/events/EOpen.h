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

#ifndef __MDS_EOPEN_H
#define __MDS_EOPEN_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class EOpen : public LogEvent {
public:
  EMetaBlob metablob;
  vector<inodeno_t> inos;

  EOpen() : LogEvent(EVENT_OPEN) { }
  EOpen(MDLog *mdlog) : 
    LogEvent(EVENT_OPEN), metablob(mdlog) { }

  void print(ostream& out) {
    out << "EOpen " << metablob << ", " << inos.size() << " open files";
  }

  void add_clean_inode(CInode *in) {
    if (!in->is_root()) {
      inode_t *pi = in->get_projected_inode();
      metablob.add_dir_context(in->get_projected_parent_dn()->get_dir());
      metablob.add_primary_dentry(in->get_projected_parent_dn(), false, 0, pi);
      inos.push_back(in->ino());
    }
  }
  void add_ino(inodeno_t ino) {
    inos.push_back(ino);
  }

  void encode(bufferlist &bl) const {
    ::encode(metablob, bl);
    ::encode(inos, bl);
  } 
  void decode(bufferlist::iterator &bl) {
    ::decode(metablob, bl);
    ::decode(inos, bl);
  }

  void update_segment();
  void replay(MDS *mds);
};

#endif
