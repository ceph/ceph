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

#ifndef __EMKNOD_H
#define __EMKNOD_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "../LogEvent.h"
#include "ETrace.h"
#include "../MDS.h"
#include "../MDStore.h"


class EMknod : public LogEvent {
 protected:
  ETrace trace;
  string symlink;

 public:
  EMknod(CInode *diri, CDentry *dn, CInode *newi) : LogEvent(EVENT_MKNOD), 
						    trace(diri),
						    symlink(newi->symlink) {
    // include new guy to the trace too
    trace.push_back(diri->ino(),
		    diri->dir->get_version(),
		    dn->get_name(),
		    newi->inode);
    ++trace.back().inode.version;  // project inode version.
  }
  EMknod() : LogEvent(EVENT_MKNOD) { }
  
  void print(ostream& out) {
    out << "mknod " << trace;
    if (symlink.length()) 
      out << " -> " << symlink;
  }

  virtual void encode_payload(bufferlist& bl) {
    trace.encode(bl);
    ::_encode(symlink, bl);
  }
  void decode_payload(bufferlist& bl, int& off) {
    trace.decode(bl, off);
    ::_decode(symlink, bl, off);
  }  

  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);

};

#endif
