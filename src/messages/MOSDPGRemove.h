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


#ifndef CEPH_MOSDPGREMOVE_H
#define CEPH_MOSDPGREMOVE_H

#include "common/hobject.h"
#include "msg/Message.h"


class MOSDPGRemove : public MessageInstance<MOSDPGRemove> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 3;

  epoch_t epoch = 0;

 public:
  vector<spg_t> pg_list;

  epoch_t get_epoch() const { return epoch; }

  MOSDPGRemove() :
    MessageInstance(MSG_OSD_PG_REMOVE, HEAD_VERSION, COMPAT_VERSION) {}
  MOSDPGRemove(epoch_t e, vector<spg_t>& l) :
    MessageInstance(MSG_OSD_PG_REMOVE, HEAD_VERSION, COMPAT_VERSION) {
    this->epoch = e;
    pg_list.swap(l);
  }
private:
  ~MOSDPGRemove() override {}

public:  
  std::string_view get_type_name() const override { return "PGrm"; }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(epoch, payload);
    encode(pg_list, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(epoch, p);
    decode(pg_list, p);
  }
  void print(ostream& out) const override {
    out << "osd pg remove(" << "epoch " << epoch << "; ";
    for (vector<spg_t>::const_iterator i = pg_list.begin();
         i != pg_list.end();
         ++i) {
      out << "pg" << *i << "; ";
    }
    out << ")";
  }
};

#endif
