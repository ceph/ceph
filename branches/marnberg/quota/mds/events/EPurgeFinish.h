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

#ifndef __EPURGE_H
#define __EPURGE_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

class EPurgeFinish : public LogEvent {
 protected:
  inodeno_t ino;

 public:
  EPurgeFinish(inodeno_t i) : 
	LogEvent(EVENT_PURGEFINISH),
	ino(i) { }
  EPurgeFinish() : LogEvent(EVENT_PURGEFINISH) { }
  
  void print(ostream& out) {
    out << "purgefinish " << ino;
  }

  virtual void encode_payload(bufferlist& bl) {
    bl.append((char*)&ino, sizeof(ino));
  }
  void decode_payload(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(ino), (char*)&ino);
  }
  
  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);

};

#endif
