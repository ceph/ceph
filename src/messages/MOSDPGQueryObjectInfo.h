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

#ifndef CEPH_MOSDPGQueryObjectInfo_H
#define CEPH_MOSDPGQueryObjectInfo_H

#include "MOSDFastDispatchOp.h"

class MOSDPGQueryObjectInfo: public MOSDFastDispatchOp {
private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 2;

public:
  enum {
    OP_GET_OBJECT_INFO = 1,      // just objects and versions
    OP_OBJECT_INFO = 2,          // result
  };
  const char *get_op_name(int o) const {
    switch (o) {
    case OP_GET_OBJECT_INFO: return "get_object_info";
    case OP_OBJECT_INFO: return "object_info";
    default: return "???";
    }
  }

  __u32 op = 0;
  ceph_tid_t rep_tid = 0;
  epoch_t map_epoch = 0, query_epoch = 0;
  pg_shard_t from;
  spg_t pgid;
  hobject_t object;
  int32_t result;

  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  epoch_t get_min_epoch() const override {
    return query_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  ceph_tid_t get_tid() const {
    return rep_tid;
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(op, p);
    decode(rep_tid, p);
    decode(map_epoch, p);
    decode(query_epoch, p);
    decode(pgid.pgid, p);
    decode(object, p);
    decode(from, p);
    decode(pgid.shard, p);
    decode(result, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(op, payload);
    encode(rep_tid, payload);
    encode(map_epoch, payload);
    if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
      // pre-nautilus OSDs do not set last_peering_reset properly
      encode(map_epoch, payload);
    } else {
      encode(query_epoch, payload);
    }
    encode(pgid.pgid, payload);
    encode(object, payload);
    encode(from, payload);
    encode(pgid.shard, payload);
    encode(result, payload);
  }

  MOSDPGQueryObjectInfo()
    : MOSDFastDispatchOp{MSG_OSD_PG_QUERY_OBJECT_INFO, HEAD_VERSION, COMPAT_VERSION} {}

  MOSDPGQueryObjectInfo(
    __u32 o,
    ceph_tid_t tid,
    pg_shard_t from,
    epoch_t e,
    epoch_t qe,
    spg_t p,
    hobject_t obj,
    int32_t res)
    : MOSDFastDispatchOp{MSG_OSD_PG_QUERY_OBJECT_INFO, HEAD_VERSION, COMPAT_VERSION},
      op(o),
      rep_tid(tid),
      map_epoch(e),
      query_epoch(qe),
      from(from),
      pgid(p),
      object(obj),
      result(res) {
  }
private:
  ~MOSDPGQueryObjectInfo() override {}

public:
  std::string_view get_type_name() const override { return "pg_query_object_info"; }
  void print(ostream& out) const override {
    out << "pg_query_object_info(" << get_op_name(op)
	<< " " << rep_tid
	<< " " << pgid
	<< " " << object
	<< " result=" << result
	<< " e " << map_epoch << "/" << query_epoch
	<< ")";
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
