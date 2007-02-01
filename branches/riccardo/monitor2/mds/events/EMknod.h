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
  //version_t pdirv;

 public:
  EMknod(CInode *in) : LogEvent(EVENT_MKNOD), 
		       trace(in) {
    //pdirv = in->get_parent_dir()->get_version();
  }
  EMknod() : LogEvent(EVENT_MKNOD) { }
  
  void print(ostream& out) {
    out << "mknod " << trace;
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
  bool has_happened(MDS *mds);  
  void replay(MDS *mds);

};

#endif
