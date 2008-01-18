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

#ifndef __MDS_ESUBTREEMAP_H
#define __MDS_ESUBTREEMAP_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class ESubtreeMap : public LogEvent {
public:
  EMetaBlob metablob;
  map<dirfrag_t, list<dirfrag_t> > subtrees;

  ESubtreeMap() : LogEvent(EVENT_SUBTREEMAP) { }
  
  void print(ostream& out) {
    out << "subtree_map " << subtrees.size() << " subtrees " 
	<< metablob;
  }

  void encode_payload(bufferlist& bl) {
    metablob._encode(bl);
    ::_encode(subtrees, bl);
  } 
  void decode_payload(bufferlist& bl, int& off) {
    metablob._decode(bl, off);
    ::_decode(subtrees, bl, off);
  }

  void replay(MDS *mds);
};

#endif
