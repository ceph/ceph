// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#ifndef __EIMPORTSTART_H
#define __EIMPORTSTART_H

#include "config.h"
#include "include/types.h"

#include "../MDS.h"

#include "EMetaBlob.h"

class EImportStart : public LogEvent {
protected:
  dirfrag_t base;
  vector<dirfrag_t> bounds;

 public:
  EMetaBlob metablob;
  bufferlist client_map;  // encoded map<__u32,entity_inst_t>
  version_t cmapv;

  EImportStart(dirfrag_t di,
	       vector<dirfrag_t>& b) : LogEvent(EVENT_IMPORTSTART), 
				       base(di), bounds(b) { }
  EImportStart() : LogEvent(EVENT_IMPORTSTART) { }
  
  void print(ostream& out) {
    out << "EImportStart " << base << " " << metablob;
  }
  
  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(base, bl);
    ::encode(metablob, bl);
    ::encode(bounds, bl);
    ::encode(cmapv, bl);
    ::encode(client_map, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(base, bl);
    ::decode(metablob, bl);
    ::decode(bounds, bl);
    ::decode(cmapv, bl);
    ::decode(client_map, bl);
  }
  
  void update_segment();
  void replay(MDS *mds);

};

#endif
