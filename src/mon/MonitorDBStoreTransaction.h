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
#ifndef CEPH_MONITOR_DB_STORE_TRANSACTION_H
#define CEPH_MONITOR_DB_STORE_TRANSACTION_H

#include <cstdint>
#include <list>
#include <memory>
#include <string>

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/types.h" // for version_t

#include "MonitorDBStoreOp.h"

namespace ceph { class Formatter; }

struct MonitorDBStoreTransaction;
typedef std::shared_ptr<MonitorDBStoreTransaction> MonitorDBStoreTransactionRef;
struct MonitorDBStoreTransaction {
  std::list<MonitorDBStoreOp> ops;
  uint64_t bytes, keys;

  MonitorDBStoreTransaction() : bytes(6 + 4 + 8*2), keys(0) {}

  enum {
    OP_PUT	= 1,
    OP_ERASE	= 2,
    OP_COMPACT = 3,
    OP_ERASE_RANGE = 4,
  };

  void put(const std::string& prefix, const std::string& key, const ceph::buffer::list& bl);
  void put(const std::string& prefix, version_t ver, const ceph::buffer::list& bl);
  void put(const std::string& prefix, const std::string& key, version_t ver);

  void erase(const std::string& prefix, const std::string& key);
  void erase(const std::string& prefix, version_t ver);

  void erase_range(const std::string& prefix, const std::string& begin,
                   const std::string& end);

  void compact_prefix(const std::string& prefix);
  void compact_range(const std::string& prefix, const std::string& start,
                     const std::string& end);

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);

  static std::list<MonitorDBStoreTransaction> generate_test_instances();

  void append(MonitorDBStoreTransactionRef other);
  void append_from_encoded(ceph::buffer::list& bl);

  bool empty() {
    return (size() == 0);
  }

  size_t size() const {
    return ops.size();
  }
  uint64_t get_keys() const {
    return keys;
  }
  uint64_t get_bytes() const {
    return bytes;
  }

  void dump(ceph::Formatter *f, bool dump_val=false) const;
};

WRITE_CLASS_ENCODER(MonitorDBStoreTransaction)

#endif /* CEPH_MONITOR_DB_TRANSACTION_H */
