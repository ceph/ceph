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

#ifndef __EEXPORT_H
#define __EEXPORT_H

#include <assert.h>
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

  virtual void encode_payload(bufferlist& bl) {
    metablob._encode(bl);
    bl.append((char*)&base, sizeof(base));
    ::_encode(bounds, bl);
  }
  void decode_payload(bufferlist& bl, int& off) {
    metablob._decode(bl, off);
    bl.copy(off, sizeof(base), (char*)&base);
    off += sizeof(base);
    ::_decode(bounds, bl, off);
  }
  
  void replay(MDS *mds);

};

#endif
