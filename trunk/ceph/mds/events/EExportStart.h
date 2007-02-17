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

#ifndef __EEXPORTSTART_H
#define __EEXPORTSTART_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "../MDS.h"

#include "EMetaBlob.h"

class EExportStart : public LogEvent {
 public:
  EMetaBlob metablob; // exported dir
 protected:
  inodeno_t      dirino;
  int dest;     // dest mds
  set<inodeno_t> bounds;

 public:
  EExportStart(CDir *dir, int d) : LogEvent(EVENT_EXPORTSTART),
				   dirino(dir->ino()),
				   dest(d) { 
    metablob.add_dir_context(dir);
  }
  EExportStart() : LogEvent(EVENT_EXPORTSTART) { }
  
  set<inodeno_t> &get_bounds() { return bounds; }

  void print(ostream& out) {
    out << "export_start " << dirino << " -> " << dest;
  }

  virtual void encode_payload(bufferlist& bl) {
    metablob._encode(bl);
    bl.append((char*)&dirino, sizeof(dirino));
    bl.append((char*)&dest, sizeof(dest));
    ::_encode(bounds, bl);
  }
  void decode_payload(bufferlist& bl, int& off) {
    metablob._decode(bl, off);
    bl.copy(off, sizeof(dirino), (char*)&dirino);
    off += sizeof(dirino);
    bl.copy(off, sizeof(dest), (char*)&dest);
    off += sizeof(dest);
    ::_decode(bounds, bl, off);
  }
  
  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);

};

#endif
