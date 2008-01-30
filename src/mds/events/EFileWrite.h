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

#ifndef __MDS_EFILEWRITE_H
#define __MDS_EFILEWRITE_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class EFileWrite : public LogEvent {
public:
  inodeno_t ino;
  __u64 size, max_size;
  utime_t mtime;

  EFileWrite() : LogEvent(EVENT_FILEWRITE) { }
  EFileWrite(MDLog *mdlog, CInode *in) : 
    LogEvent(EVENT_FILEWRITE) { 
    ino = in->inode.ino;
    size = in->inode.size;
    max_size = in->inode.max_size;
    mtime = in->inode.mtime;
  }

  void print(ostream& out) {
    out << "EFileWrite " << ino
	<< " size " << size
	<< " max " << max_size
	<< " mtime " << mtime;
  }

  void encode_payload(bufferlist& bl) {
    ::_encode(ino, bl);
    ::_encode(size, bl);
    ::_encode(max_size, bl);
    ::_encode(mtime, bl);
  } 
  void decode_payload(bufferlist& bl, int& off) {
    ::_decode(ino, bl, off);
    ::_decode(size, bl, off);
    ::_decode(max_size, bl, off);
    ::_decode(mtime, bl, off);
  }

  void update_segment();
  void replay(MDS *mds);
};

#endif
