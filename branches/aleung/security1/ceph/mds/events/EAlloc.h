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

#ifndef __MDS_EALLOC_H
#define __MDS_EALLOC_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "../LogEvent.h"
#include "../IdAllocator.h"

#define EALLOC_EV_ALLOC  1
#define EALLOC_EV_FREE   2

class EAlloc : public LogEvent {
 protected:
  int  idtype;
  idno_t id;
  int  what;  // alloc or dealloc
  version_t table_version;

 public:
  EAlloc() : LogEvent(EVENT_ALLOC) { }
  EAlloc(int idtype, idno_t id, int what, version_t v) :
    LogEvent(EVENT_ALLOC) {
    this->idtype = idtype;
    this->id = id;
    this->what = what;
    this->table_version = v;
  }
  
  void encode_payload(bufferlist& bl) {
    bl.append((char*)&idtype, sizeof(idtype));
    bl.append((char*)&id, sizeof(id));
    bl.append((char*)&what, sizeof(what));
    bl.append((char*)&table_version, sizeof(table_version));
  }
  void decode_payload(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(idtype), (char*)&idtype);
    off += sizeof(idtype);
    bl.copy(off, sizeof(id), (char*)&id);
    off += sizeof(id);
    bl.copy(off, sizeof(what), (char*)&what);
    off += sizeof(what);
    bl.copy(off, sizeof(table_version), (char*)&table_version);
    off += sizeof(table_version);
  }


  void print(ostream& out) {
    if (what == EALLOC_EV_ALLOC) 
      out << "EAlloc alloc " << hex << id << dec << " tablev " << table_version;
    else
      out << "EAlloc dealloc " << hex << id << dec << " tablev " << table_version;
  }
  

  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);
  
};

#endif
