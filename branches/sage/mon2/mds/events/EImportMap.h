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

#ifndef __MDS_EIMPORTMAP_H
#define __MDS_EIMPORTMAP_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class EImportMap : public LogEvent {
public:
  EMetaBlob metablob;
  set<dirfrag_t> imports;
  map<dirfrag_t, set<dirfrag_t> > bounds;

  EImportMap() : LogEvent(EVENT_IMPORTMAP) { }
  
  void print(ostream& out) {
    out << "import_map " << imports.size() << " imports " 
	<< metablob;
  }

  void encode_payload(bufferlist& bl) {
    metablob._encode(bl);
    ::_encode(imports, bl);
    for (set<dirfrag_t>::iterator p = imports.begin();
	 p != imports.end();
	 ++p) {
      ::_encode(bounds[*p], bl);
      if (bounds[*p].empty())
	bounds.erase(*p);
    }
  } 
  void decode_payload(bufferlist& bl, int& off) {
    metablob._decode(bl, off);
    ::_decode(imports, bl, off);
    for (set<dirfrag_t>::iterator p = imports.begin();
	 p != imports.end();
	 ++p) {
      ::_decode(bounds[*p], bl, off);
      if (bounds[*p].empty())
	bounds.erase(*p);
    }
  }

  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);
};

#endif
