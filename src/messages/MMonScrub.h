// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2013 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#ifndef CEPH_MMONSCRUB_H
#define CEPH_MMONSCRUB_H

#include "msg/Message.h"
#include "mon/mon_types.h"

class MMonScrub : public MessageInstance<MMonScrub> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 2;

public:
  typedef enum {
    OP_SCRUB = 1,         // leader->peon: scrub (a range of) keys
    OP_RESULT = 2,        // peon->leader: result of a scrub
  } op_type_t;

  static const char *get_opname(op_type_t op) {
    switch (op) {
    case OP_SCRUB: return "scrub";
    case OP_RESULT: return "result";
    default: ceph_abort_msg("unknown op type"); return NULL;
    }
  }

  op_type_t op = OP_SCRUB;
  version_t version = 0;
  ScrubResult result;
  int32_t num_keys;
  pair<string,string> key;

  MMonScrub()
    : MessageInstance(MSG_MON_SCRUB, HEAD_VERSION, COMPAT_VERSION),
      num_keys(-1)
  { }

  MMonScrub(op_type_t op, version_t v, int32_t num_keys)
    : MessageInstance(MSG_MON_SCRUB, HEAD_VERSION, COMPAT_VERSION),
      op(op), version(v), num_keys(num_keys)
  { }

  std::string_view get_type_name() const override { return "mon_scrub"; }

  void print(ostream& out) const override {
    out << "mon_scrub(" << get_opname((op_type_t)op);
    out << " v " << version;
    if (op == OP_RESULT)
      out << " " << result;
    out << " num_keys " << num_keys;
    out << " key (" << key << ")";
    out << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    uint8_t o = op;
    encode(o, payload);
    encode(version, payload);
    encode(result, payload);
    encode(num_keys, payload);
    encode(key, payload);
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    uint8_t o;
    decode(o, p);
    op = (op_type_t)o;
    decode(version, p);
    decode(result, p);
    decode(num_keys, p);
    decode(key, p);
  }
};

#endif /* CEPH_MMONSCRUB_H */
