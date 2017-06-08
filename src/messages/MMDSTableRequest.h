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


#ifndef CEPH_MMDSTABLEREQUEST_H
#define CEPH_MMDSTABLEREQUEST_H

#include "msg/Message.h"
#include "mds/mds_table_types.h"

class MMDSTableRequest : public Message {
 public:
  __u16 table;
  __s16 op;
  uint64_t reqid;
  bufferlist bl;

  MMDSTableRequest() : Message(MSG_MDS_TABLE_REQUEST) {}
  MMDSTableRequest(int tab, int o, uint64_t r, version_t v=0) : 
    Message(MSG_MDS_TABLE_REQUEST),
    table(tab), op(o), reqid(r) {
    set_tid(v);
  }
private:
  ~MMDSTableRequest() {}

public:  
  virtual const char *get_type_name() const { return "mds_table_request"; }
  void print(ostream& o) const {
    o << "mds_table_request(" << get_mdstable_name(table)
      << " " << get_mdstableserver_opname(op);
    if (reqid) o << " " << reqid;
    if (get_tid()) o << " tid " << get_tid();
    if (bl.length()) o << " " << bl.length() << " bytes";
    o << ")";
  }

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(table, p);
    ::decode(op, p);
    ::decode(reqid, p);
    ::decode(bl, p);
  }

  virtual void encode_payload(uint64_t features) {
    ::encode(table, payload);
    ::encode(op, payload);
    ::encode(reqid, payload);
    ::encode(bl, payload);
  }
};

#endif
