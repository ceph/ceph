// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
  set<inodeno_t> imports;
  set<inodeno_t> exports;
  set<inodeno_t> hashdirs;
  map<inodeno_t, set<inodeno_t> > nested_exports;

  EImportMap() : LogEvent(EVENT_IMPORTMAP) { }
  
  void print(ostream& out) {
    out << "import_map " << imports.size() << " imports, " 
	<< exports.size() << " exports";
  }

  void encode_payload(bufferlist& bl) {
    metablob._encode(bl);
  } 
  void decode_payload(bufferlist& bl, int& off) {
    metablob._decode(bl, off);
  }

  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);
};

#endif
