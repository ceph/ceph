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


class MOSDPGRemove final : public Message {
private:
  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 3;

  epoch_t epoch = 0;

 public:
  std::vector<spg_t> pg_list;

  epoch_t get_epoch() const { return epoch; }

  MOSDPGRemove() :
    Message{MSG_OSD_PG_REMOVE, HEAD_VERSION, COMPAT_VERSION} {}
  MOSDPGRemove(epoch_t e, std::vector<spg_t>& l) :
    Message{MSG_OSD_PG_REMOVE, HEAD_VERSION, COMPAT_VERSION} {
    this->epoch = e;
    pg_list.swap(l);
  }
private:
  ~MOSDPGRemove() final {}

public:
  std::string_view get_type_name() const override { return "PGrm"; }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(epoch, payload);
    encode(pg_list, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(epoch, p);
    decode(pg_list, p);
  }
  void print(std::ostream& out) const override {
    out << "osd pg remove(" << "epoch " << epoch << "; ";
    for (auto i = pg_list.begin(); i != pg_list.end(); ++i) {
      out << "pg" << *i << "; ";
    }
    out << ")";
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
