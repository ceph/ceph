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

#ifndef CEPH_MMONMAP_H
#define CEPH_MMONMAP_H

#include "include/encoding.h"
#include "include/ceph_features.h"
#include "msg/Message.h"
#include "msg/MessageRef.h"
#include "mon/MonMap.h"

class MMonMap final : public Message {
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 0;
public:
  ceph::buffer::list monmapbl;
  epoch_t monmap_epoch {0};
  std::set<int> quorum;

  MMonMap() :
    Message{CEPH_MSG_MON_MAP, HEAD_VERSION, COMPAT_VERSION} { }
  explicit MMonMap(ceph::buffer::list &bl, epoch_t e, std::set<int> q) :
    Message{CEPH_MSG_MON_MAP, HEAD_VERSION, COMPAT_VERSION} {
    monmapbl = std::move(bl);
    monmap_epoch = e;
    quorum = std::move(q);
  }
private:
  ~MMonMap() final {}

public:
  std::string_view get_type_name() const override { return "mon_map"; }

  void encode_payload(uint64_t features) override { 
    header.version = HEAD_VERSION;
    if (monmapbl.length() &&
	((features & CEPH_FEATURE_MONENC) == 0 ||
	 (features & CEPH_FEATURE_MSG_ADDR2) == 0)) {
      // reencode old-format monmap
      MonMap t;
      t.decode(monmapbl);
      monmapbl.clear();
      t.encode(monmapbl, features);
    }

    using ceph::encode;
    encode(monmapbl, payload);
    encode(monmap_epoch, payload);
    encode(quorum, payload);
  }
  void decode_payload() override { 
    using ceph::decode;
    auto p = payload.cbegin();
    decode(monmapbl, p);
    if(header.version >= 2) {
      decode(monmap_epoch, p);
      decode(quorum, p);
    }
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
