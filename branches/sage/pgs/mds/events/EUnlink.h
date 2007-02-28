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

#ifndef __EUNLINK_H
#define __EUNLINK_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "../LogEvent.h"
#include "EMetaBlob.h"

#include "../CInode.h"
#include "../CDentry.h"
#include "../CDir.h"

/// help rewrite me

class EUnlink : public LogEvent {
 protected:
  version_t dirv;
  string dname;

 public:
  EMetaBlob metaglob;
  
  /*
  EUnlink(CDir *dir, CDentry* dn, CInode *in) :
    LogEvent(EVENT_UNLINK),
    diritrace(dir->inode), 
    dirv(dir->get_version()),
    dname(dn->get_name()),
    inodetrace(in) {}
  */
  EUnlink() : LogEvent(EVENT_UNLINK) { }
  
  virtual void encode_payload(bufferlist& bl) {
  /*
    diritrace.encode(bl);
    bl.append((char*)&dirv, sizeof(dirv));
    ::_encode(dname, bl);
    inodetrace.encode(bl);
  */
  }
  void decode_payload(bufferlist& bl, int& off) {
    /*
    diritrace.decode(bl,off);
    bl.copy(off, sizeof(dirv), (char*)&dirv);
    off += sizeof(dirv);
    ::_decode(dname, bl, off);
    inodetrace.decode(bl, off);
  */
  }
  
  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);
};

#endif
