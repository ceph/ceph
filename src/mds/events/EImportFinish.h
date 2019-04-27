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

#ifndef CEPH_EIMPORTFINISH_H
#define CEPH_EIMPORTFINISH_H

#include "common/config.h"
#include "include/types.h"

#include "../MDSRank.h"
#include "../LogEvent.h"

class EImportFinish : public LogEvent {
 protected:
  dirfrag_t base; // imported dir
  bool success;

 public:
  EImportFinish(CDir *dir, bool s) : LogEvent(EVENT_IMPORTFINISH), 
				     base(dir->dirfrag()),
				     success(s) { }
  EImportFinish() : LogEvent(EVENT_IMPORTFINISH), base(), success(false) { }
  
  void print(ostream& out) const override {
    out << "EImportFinish " << base;
    if (success)
      out << " success";
    else
      out << " failed";
  }

  void encode(bufferlist& bl, uint64_t features) const override;
  void decode(bufferlist::const_iterator &bl) override;
  void dump(Formatter *f) const override;
  static void generate_test_instances(std::list<EImportFinish*>& ls);
  
  void replay(MDSRank *mds) override;

};
WRITE_CLASS_ENCODER_FEATURES(EImportFinish)

#endif
