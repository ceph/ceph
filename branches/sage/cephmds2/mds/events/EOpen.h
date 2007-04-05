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

#ifndef __MDS_EOPEN_H
#define __MDS_EOPEN_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class EOpen : public LogEvent {
public:
  EMetaBlob metablob;
  inodeno_t ino;

  EOpen() : LogEvent(EVENT_OPEN) { }
  EOpen(CInode *in) : LogEvent(EVENT_OPEN),
		      ino(in->ino()) {
    metablob.add_primary_dentry(in->get_parent_dn(), false);
  }
  void print(ostream& out) {
    out << "EOpen " << ino << " " << metablob;
  }

  void encode_payload(bufferlist& bl) {
    ::_encode(ino, bl);
    metablob._encode(bl);
  } 
  void decode_payload(bufferlist& bl, int& off) {
    ::_decode(ino, bl, off);
    metablob._decode(bl, off);
  }

  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);
};

#endif
