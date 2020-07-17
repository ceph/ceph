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

#include "mds/mds_table_types.h"
#include "messages/MMDSOp.h"

class MMDSTableRequest : public MMDSOp {
public:
  __u16 table = 0;
  __s16 op = 0;
  uint64_t reqid = 0;
  ceph::buffer::list bl;

protected:
  MMDSTableRequest() : MMDSOp{MSG_MDS_TABLE_REQUEST} {}
  MMDSTableRequest(int tab, int o, uint64_t r, version_t v=0) : 
    MMDSOp{MSG_MDS_TABLE_REQUEST},
    table(tab), op(o), reqid(r) {
    set_tid(v);
  }
  ~MMDSTableRequest() override {}

public:
  std::string_view get_type_name() const override { return "mds_table_request"; }
  void print(std::ostream& o) const override {
    o << "mds_table_request(" << get_mdstable_name(table)
      << " " << get_mdstableserver_opname(op);
    if (reqid) o << " " << reqid;
    if (get_tid()) o << " tid " << get_tid();
    if (bl.length()) o << " " << bl.length() << " bytes";
    o << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(table, p);
    decode(op, p);
    decode(reqid, p);
    decode(bl, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(table, payload);
    encode(op, payload);
    encode(reqid, payload);
    encode(bl, payload);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
