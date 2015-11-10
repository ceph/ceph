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

#ifndef CEPH_EEXPORT_H
#define CEPH_EEXPORT_H

#include "common/config.h"
#include "include/types.h"

#include "../MDSRank.h"

#include "EMetaBlob.h"
#include "../LogEvent.h"

class EExport : public LogEvent {
public:
  EMetaBlob metablob; // exported dir
protected:
  dirfrag_t      base;
  set<dirfrag_t> bounds;
  
public:
  EExport() : LogEvent(EVENT_EXPORT) { }
  EExport(MDLog *mdlog, CDir *dir) : 
    LogEvent(EVENT_EXPORT), metablob(mdlog),
    base(dir->dirfrag()) { }
  
  set<dirfrag_t> &get_bounds() { return bounds; }
  
  void print(ostream& out) const {
    out << "EExport " << base << " " << metablob;
  }

  EMetaBlob *get_metablob() { return &metablob; }

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<EExport*>& ls);
  void replay(MDSRank *mds);

};
WRITE_CLASS_ENCODER_FEATURES(EExport)

#endif
