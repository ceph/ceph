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

#include "MonitorDBStoreTransaction.h"

#include <sstream>

#include "common/Formatter.h"
#include "include/encoding_list.h"

void MonitorDBStoreTransaction::put(const std::string& prefix, const std::string& key, const ceph::buffer::list& bl) {
  ops.push_back(MonitorDBStoreOp(OP_PUT, prefix, key, bl));
  ++keys;
  bytes += ops.back().approx_size();
}

void MonitorDBStoreTransaction::put(const std::string& prefix, version_t ver, const ceph::buffer::list& bl) {
  std::ostringstream os;
  os << ver;
  put(prefix, os.str(), bl);
}

void MonitorDBStoreTransaction::put(const std::string& prefix, const std::string& key, version_t ver) {
  using ceph::encode;
  ceph::buffer::list bl;
  encode(ver, bl);
  put(prefix, key, bl);
}

void MonitorDBStoreTransaction::erase(const std::string& prefix, const std::string& key) {
  ops.push_back(MonitorDBStoreOp(OP_ERASE, prefix, key));
  ++keys;
  bytes += ops.back().approx_size();
}

void MonitorDBStoreTransaction::erase(const std::string& prefix, version_t ver) {
  std::ostringstream os;
  os << ver;
  erase(prefix, os.str());
}

void MonitorDBStoreTransaction::erase_range(const std::string& prefix, const std::string& begin,
                                              const std::string& end) {
  ops.push_back(MonitorDBStoreOp(OP_ERASE_RANGE, prefix, begin, end));
  ++keys;
  bytes += ops.back().approx_size();
}

void MonitorDBStoreTransaction::compact_prefix(const std::string& prefix) {
  ops.push_back(MonitorDBStoreOp(OP_COMPACT, prefix, {}));
}

void MonitorDBStoreTransaction::compact_range(const std::string& prefix, const std::string& start,
                                                const std::string& end) {
  ops.push_back(MonitorDBStoreOp(OP_COMPACT, prefix, start, end));
}

void MonitorDBStoreTransaction::encode(ceph::buffer::list& bl) const {
  ENCODE_START(2, 1, bl);
  encode(ops, bl);
  encode(bytes, bl);
  encode(keys, bl);
  ENCODE_FINISH(bl);
}

void MonitorDBStoreTransaction::decode(ceph::buffer::list::const_iterator& bl) {
  DECODE_START(2, bl);
  decode(ops, bl);
  if (struct_v >= 2) {
    decode(bytes, bl);
    decode(keys, bl);
  }
  DECODE_FINISH(bl);
}

std::list<MonitorDBStoreTransaction> MonitorDBStoreTransaction::generate_test_instances() {
  std::list<MonitorDBStoreTransaction> ls;
  ls.emplace_back();
  ls.emplace_back();
  ceph::buffer::list bl;
  bl.append("value");
  ls.back().put("prefix", "key", bl);
  ls.back().erase("prefix2", "key2");
  ls.back().erase_range("prefix3", "key3", "key4");
  ls.back().compact_prefix("prefix3");
  ls.back().compact_range("prefix4", "from", "to");
  return ls;
}

void MonitorDBStoreTransaction::append(MonitorDBStoreTransactionRef other) {
  ops.splice(ops.end(), other->ops);
  keys += other->keys;
  bytes += other->bytes;
}

void MonitorDBStoreTransaction::append_from_encoded(ceph::buffer::list& bl) {
  auto other(std::make_shared<MonitorDBStoreTransaction>());
  auto it = bl.cbegin();
  other->decode(it);
  append(other);
}

void MonitorDBStoreTransaction::dump(ceph::Formatter *f, bool dump_val) const {
  f->open_object_section("transaction");
  f->open_array_section("ops");
  int op_num = 0;
  for (auto it = ops.begin(); it != ops.end(); ++it) {
    const MonitorDBStoreOp& op = *it;
    f->open_object_section("op");
    f->dump_int("op_num", op_num++);
    switch (op.type) {
    case OP_PUT:
    {
      f->dump_string("type", "PUT");
      f->dump_string("prefix", op.prefix);
      f->dump_string("key", op.key);
      f->dump_unsigned("length", op.bl.length());
      if (dump_val) {
        std::ostringstream os;
        op.bl.hexdump(os);
        f->dump_string("bl", os.str());
      }
    }
    break;
    case OP_ERASE:
    {
      f->dump_string("type", "ERASE");
      f->dump_string("prefix", op.prefix);
      f->dump_string("key", op.key);
    }
    break;
    case OP_ERASE_RANGE:
    {
      f->dump_string("type", "ERASE_RANGE");
      f->dump_string("prefix", op.prefix);
      f->dump_string("start", op.key);
      f->dump_string("end", op.endkey);
    }
    break;
    case OP_COMPACT:
    {
      f->dump_string("type", "COMPACT");
      f->dump_string("prefix", op.prefix);
      f->dump_string("start", op.key);
      f->dump_string("end", op.endkey);
    }
    break;
    default:
    {
      f->dump_string("type", "unknown");
      f->dump_unsigned("op_code", op.type);
      break;
    }
    }
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("num_keys", keys);
  f->dump_unsigned("num_bytes", bytes);
  f->close_section();
}
