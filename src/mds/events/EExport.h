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

#include "config.h"
#include "include/types.h"

#include "../MDS.h"

#include "EMetaBlob.h"

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
  
  void print(ostream& out) {
    out << "EExport " << base << " " << metablob;
  }

  void encode(bufferlist& bl) const {
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    ::encode(stamp, bl);
    ::encode(metablob, bl);
    ::encode(base, bl);
    ::encode(bounds, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    if (struct_v >= 2)
      ::decode(stamp, bl);
    ::decode(metablob, bl);
    ::decode(base, bl);
    ::decode(bounds, bl);
  }
  
  void replay(MDS *mds);

};

#endif
