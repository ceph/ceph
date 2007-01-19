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

#ifndef __EMKDIR_H
#define __EMKDIR_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "ETrace.h"
#include "../MDS.h"
#include "../MDStore.h"


class EMkdir : public LogEvent {
 protected:
  ETrace trace;
  //version_t pdirv;

 public:
  EMkdir(CDir *dir) : LogEvent(EVENT_MKDIR),
		      trace(dir->inode) {
    //pdirv = dir->inode->get_parent_dir()->get_version();
  }
  EMkdir() : LogEvent(EVENT_MKDIR) { }
  
  void print(ostream& out) {
    out << "mkdir ";
    trace.print(out);
  }

  virtual void encode_payload(bufferlist& bl) {
    trace.encode(bl);
    //bl.append((char*)&pdirv, sizeof(pdirv));
  }
  void decode_payload(bufferlist& bl, int& off) {
    trace.decode(bl, off);
    //bl.copy(off, sizeof(pdirv), (char*)&pdirv);
    //off += sizeof(pdirv);
  }
  
  bool can_expire(MDS *mds);
  void retire(MDS *mds, Context *c);

  // recovery
  bool has_happened(MDS *mds);  
  void replay(MDS *mds);

};

#endif
