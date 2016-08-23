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

#ifndef CEPH_MDS_ETABLECLIENT_H
#define CEPH_MDS_ETABLECLIENT_H

#include "common/config.h"
#include "include/types.h"

#include "../mds_table_types.h"
#include "../LogEvent.h"

struct ETableClient : public LogEvent {
  __u16 table;
  __s16 op;
  version_t tid;

  ETableClient() : LogEvent(EVENT_TABLECLIENT), table(0), op(0), tid(0) { }
  ETableClient(int t, int o, version_t ti) :
    LogEvent(EVENT_TABLECLIENT),
    table(t), op(o), tid(ti) { }

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ETableClient*>& ls);

  void print(ostream& out) const {
    out << "ETableClient " << get_mdstable_name(table) << " " << get_mdstableserver_opname(op);
    if (tid) out << " tid " << tid;
  }  

  //void update_segment();
  void replay(MDSRank *mds);  
};
WRITE_CLASS_ENCODER_FEATURES(ETableClient)

#endif
