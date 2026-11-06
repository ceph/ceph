// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#ifndef CEPH_MONITOR_DB_STORE_OP_H
#define CEPH_MONITOR_DB_STORE_OP_H

#include <cstdint>
#include <list>
#include <string>

#include "include/buffer.h"
#include "include/encoding.h"

namespace ceph { class Formatter; }

struct MonitorDBStoreOp {
  uint8_t type;
  std::string prefix;
  std::string key, endkey;
  ceph::buffer::list bl;

  MonitorDBStoreOp()
    : type(0) { }
  MonitorDBStoreOp(int t, const std::string& p, const std::string& k)
    : type(t), prefix(p), key(k) { }
  MonitorDBStoreOp(int t, const std::string& p, const std::string& k, const ceph::buffer::list& b)
    : type(t), prefix(p), key(k), bl(b) { }
  MonitorDBStoreOp(int t, const std::string& p, const std::string& start, const std::string& end)
    : type(t), prefix(p), key(start), endkey(end) { }

  void encode(ceph::buffer::list& encode_bl) const;
  void decode(ceph::buffer::list::const_iterator& decode_bl);

  void dump(ceph::Formatter *f) const;

  int approx_size() const {
    return 6 + 1 +
      4 + prefix.size() +
      4 + key.size() +
      4 + endkey.size() +
      4 + bl.length();
  }

  static std::list<MonitorDBStoreOp> generate_test_instances();
};

WRITE_CLASS_ENCODER(MonitorDBStoreOp)

#endif /* CEPH_MONITOR_DB_OP_H */
