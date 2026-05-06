// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#pragma once

#include "include/fs_types.h"
#include "../LogEvent.h"
#include "../Quarantine.h"

class EQuarantineStart : public QuarantineEvent {
public:
  EQuarantineStart() : QuarantineEvent(EVENT_QUARANTINESTART) { }
  
  void print(std::ostream& out) const override {
    out << "EQuarantineStart ino:" << QuarantineEvent::subvol_ino << ", "
        << "op:" << QuarantineEvent::qtine_op;
  }

  void encode(bufferlist& bl, uint64_t features) const override;
  void decode(bufferlist::const_iterator& bl) override;
  void dump(Formatter *f) const override;
  static std::list<EQuarantineStart> generate_test_instances();

  void update_segment() override {};
  void replay(MDSRank *mds) override;
};
WRITE_CLASS_ENCODER_FEATURES(EQuarantineStart)
