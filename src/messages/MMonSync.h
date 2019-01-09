// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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
#ifndef CEPH_MMONSYNC_H
#define CEPH_MMONSYNC_H

#include "msg/Message.h"

class MMonSync : public MessageInstance<MMonSync> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 2;

public:
  /**
  * Operation types
  */
  enum {
    OP_GET_COOKIE_FULL = 1,   // -> start a session (full scan)
    OP_GET_COOKIE_RECENT = 2, // -> start a session (only recent paxos events)
    OP_COOKIE = 3,            // <- pass the iterator cookie, or
    OP_GET_CHUNK = 4,         // -> get some keys
    OP_CHUNK = 5,             // <- return some keys
    OP_LAST_CHUNK = 6,        // <- return the last set of keys
    OP_NO_COOKIE = 8,         // <- sorry, no cookie
  };

  /**
  * Obtain a string corresponding to the operation type @p op
  *
  * @param op Operation type
  * @returns A string
  */
  static const char *get_opname(int op) {
    switch (op) {
    case OP_GET_COOKIE_FULL: return "get_cookie_full";
    case OP_GET_COOKIE_RECENT: return "get_cookie_recent";
    case OP_COOKIE: return "cookie";
    case OP_GET_CHUNK: return "get_chunk";
    case OP_CHUNK: return "chunk";
    case OP_LAST_CHUNK: return "last_chunk";
    case OP_NO_COOKIE: return "no_cookie";
    default: ceph_abort_msg("unknown op type"); return NULL;
    }
  }

  uint32_t op = 0;
  uint64_t cookie = 0;
  version_t last_committed = 0;
  pair<string,string> last_key;
  bufferlist chunk_bl;
  entity_inst_t reply_to;

  MMonSync()
    : MessageInstance(MSG_MON_SYNC, HEAD_VERSION, COMPAT_VERSION)
  { }

  MMonSync(uint32_t op, uint64_t c = 0)
    : MessageInstance(MSG_MON_SYNC, HEAD_VERSION, COMPAT_VERSION),
      op(op),
      cookie(c),
      last_committed(0)
  { }

  std::string_view get_type_name() const override { return "mon_sync"; }

  void print(ostream& out) const override {
    out << "mon_sync(" << get_opname(op);
    if (cookie)
      out << " cookie " << cookie;
    if (last_committed > 0)
      out << " lc " << last_committed;
    if (chunk_bl.length())
      out << " bl " << chunk_bl.length() << " bytes";
    if (!last_key.first.empty() || !last_key.second.empty())
      out << " last_key " << last_key.first << "," << last_key.second;
    out << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(op, payload);
    encode(cookie, payload);
    encode(last_committed, payload);
    encode(last_key.first, payload);
    encode(last_key.second, payload);
    encode(chunk_bl, payload);
    encode(reply_to, payload, features);
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(op, p);
    decode(cookie, p);
    decode(last_committed, p);
    decode(last_key.first, p);
    decode(last_key.second, p);
    decode(chunk_bl, p);
    decode(reply_to, p);
  }
};

#endif /* CEPH_MMONSYNC_H */
