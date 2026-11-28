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

#include "MonitorDBStoreOp.h"

#include "common/Formatter.h"
#include "include/encoding_string.h"

void MonitorDBStoreOp::encode(ceph::buffer::list& encode_bl) const {
  ENCODE_START(2, 1, encode_bl);
  encode(type, encode_bl);
  encode(prefix, encode_bl);
  encode(key, encode_bl);
  encode(bl, encode_bl);
  encode(endkey, encode_bl);
  ENCODE_FINISH(encode_bl);
}

void MonitorDBStoreOp::decode(ceph::buffer::list::const_iterator& decode_bl) {
  DECODE_START(2, decode_bl);
  decode(type, decode_bl);
  decode(prefix, decode_bl);
  decode(key, decode_bl);
  decode(bl, decode_bl);
  if (struct_v >= 2)
    decode(endkey, decode_bl);
  DECODE_FINISH(decode_bl);
}

void MonitorDBStoreOp::dump(ceph::Formatter *f) const {
  f->dump_int("type", type);
  f->dump_string("prefix", prefix);
  f->dump_string("key", key);
  if (endkey.length()) {
    f->dump_string("endkey", endkey);
  }
}

std::list<MonitorDBStoreOp> MonitorDBStoreOp::generate_test_instances() {
  std::list<MonitorDBStoreOp> ls;
  ls.emplace_back();
  // we get coverage here from the Transaction instances
  return ls;
}
