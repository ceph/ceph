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

#include "ETraced.h"
#include "../MDStore.h"


class EMknod : public ETraced {
 protected:
 public:
  EMknod(CInode *in) : ETraced(EVENT_MKNOD, in) {
  }
  EMknod() : ETraced(EVENT_MKNOD) { }
  
  void print(ostream& out) {
    out << "mknod ";
    ETraced::print(out);
  }

  virtual void encode_payload(bufferlist& bl) {
    encode_trace(bl);
  }
  void decode_payload(bufferlist& bl, int& off) {
    decode_trace(bl, off);
  }
  
  bool can_expire(MDS *mds) {
    // am i obsolete?
    CInode *diri = mds->mdcache->get_inode( trace.back().dirino );
    if (!diri) return true;
	CDir *dir = diri->dir;
	if (!dir) return true;

    if (!dir->is_auth()) return true;     // not mine!
    if (dir->is_frozen()) return true;    // frozen -> exporting -> obsolete? FIXME
    
    if (!dir->is_dirty()) return true;

    if (dir->get_committing_version() > trace.back().dirv)
      return true;

    return false;
  }

  virtual void retire(MDS *mds, Context *c) {
    // commit directory
    CInode *in = mds->mdcache->get_inode( trace.back().dirino );
    assert(in);
    CDir *dir = in->dir;
    assert(dir);

    dout(10) << "EMknod committing dir " << *dir << endl;
    mds->mdstore->commit_dir(dir, c);
  }
  
};

#endif
