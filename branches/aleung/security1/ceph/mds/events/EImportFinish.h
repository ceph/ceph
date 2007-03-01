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

#ifndef __EIMPORTFINISH_H
#define __EIMPORTFINISH_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "../MDS.h"

class EImportFinish : public LogEvent {
 protected:
  inodeno_t dirino; // imported dir
  bool success;

 public:
  EImportFinish(CDir *dir, bool s) : LogEvent(EVENT_IMPORTFINISH), 
				     dirino(dir->ino()),
				     success(s) { }
  EImportFinish() : LogEvent(EVENT_IMPORTFINISH) { }
  
  void print(ostream& out) {
    out << "import_finish " << dirino;
    if (success)
      out << " success";
    else
      out << " failed";
  }

  virtual void encode_payload(bufferlist& bl) {
    bl.append((char*)&dirino, sizeof(dirino));
    bl.append((char*)&success, sizeof(success));
  }
  void decode_payload(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(dirino), (char*)&dirino);
    off += sizeof(dirino);
    bl.copy(off, sizeof(success), (char*)&success);
    off += sizeof(success);
  }
  
  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);

};

#endif
