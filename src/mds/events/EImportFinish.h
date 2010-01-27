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

#ifndef __EIMPORTFINISH_H
#define __EIMPORTFINISH_H

#include "config.h"
#include "include/types.h"

#include "../MDS.h"

class EImportFinish : public LogEvent {
 protected:
  dirfrag_t base; // imported dir
  bool success;

 public:
  EImportFinish(CDir *dir, bool s) : LogEvent(EVENT_IMPORTFINISH), 
				     base(dir->dirfrag()),
				     success(s) { }
  EImportFinish() : LogEvent(EVENT_IMPORTFINISH) { }
  
  void print(ostream& out) {
    out << "EImportFinish " << base;
    if (success)
      out << " success";
    else
      out << " failed";
  }

  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(base, bl);
    ::encode(success, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(base, bl);
    ::decode(success, bl);
  }
  
  void replay(MDS *mds);

};

#endif
