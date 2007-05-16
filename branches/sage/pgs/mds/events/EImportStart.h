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

#ifndef __EIMPORTSTART_H
#define __EIMPORTSTART_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "../MDS.h"

#include "EMetaBlob.h"

class EImportStart : public LogEvent {
protected:
  dirfrag_t base;
  list<dirfrag_t> bounds;

 public:
  EMetaBlob metablob;

  EImportStart(dirfrag_t di,
	       list<dirfrag_t>& b) : LogEvent(EVENT_IMPORTSTART), 
				     base(di), bounds(b) { }
  EImportStart() : LogEvent(EVENT_IMPORTSTART) { }
  
  void print(ostream& out) {
    out << "EImportStart " << base << " " << metablob;
  }
  
  virtual void encode_payload(bufferlist& bl) {
    bl.append((char*)&base, sizeof(base));
    metablob._encode(bl);
    ::_encode(bounds, bl);
  }
  void decode_payload(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(base), (char*)&base);
    off += sizeof(base);
    metablob._decode(bl, off);
    ::_decode(bounds, bl, off);
  }
  
  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);

};

#endif
