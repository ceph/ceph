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

#ifndef __EEXPORTFINISH_H
#define __EEXPORTFINISH_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "../MDS.h"

class EExportFinish : public LogEvent {
 protected:
  inodeno_t dirino; // exported dir

 public:
  EExportFinish(CDir *dir) : LogEvent(EVENT_EXPORTFINISH), 
			     dirino(dir->ino()) { }
  EExportFinish() : LogEvent(EVENT_EXPORTFINISH) { }
  
  void print(ostream& out) {
    out << "export_finish " << dirino;
  }

  virtual void encode_payload(bufferlist& bl) {
    bl.append((char*)&dirino, sizeof(dirino));
  }
  void decode_payload(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(dirino), (char*)&dirino);
    off += sizeof(dirino);
  }
  
  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);

};

#endif
