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

#ifndef __MDS_EFILEACCESS_H
#define __MDS_EFILEACCESS_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class EFileAccess : public LogEvent {
public:
  inodeno_t ino;
  utime_t atime;

  EFileAccess() : LogEvent(EVENT_FILEACCESS) { }
  EFileAccess(MDLog *mdlog, CInode *in) : 
    LogEvent(EVENT_FILEACCESS) { 
    ino = in->inode.ino;
    atime = in->inode.atime;
  }

  void print(ostream& out) {
    out << "EFileAccess " << ino
	<< " atime " << atime;
  }

  void encode_payload(bufferlist& bl) {
    ::_encode(ino, bl);
    ::_encode(atime, bl);
  } 
  void decode_payload(bufferlist& bl, int& off) {
    ::_decode(ino, bl, off);
    ::_decode(atime, bl, off);
  }

  void update_segment();
  void replay(MDS *mds);
};

#endif
