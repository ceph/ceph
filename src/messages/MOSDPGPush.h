// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef MOSDPGPUSH_H
#define MOSDPGPUSH_H

#include "MOSDFastDispatchOp.h"
#include "include/intarith.h"
#include "include/scope_guard.h"

class MOSDPGPush : public MOSDFastDispatchOp {
private:
  static constexpr int HEAD_VERSION = 5;
  static constexpr int COMPAT_VERSION = 2;
  // the last version with the pushes' data in the payload; from v5 it is in
  // the data segment, which msgr2 receives page aligned
  static constexpr int INLINE_DATA_VERSION = 4;
  // a push of at least DATA_ALIGN_MIN_LEN starts at a multiple of DATA_ALIGN
  // in the data segment, so that the store can write it without copying it
  // into aligned buffers; smaller ones are not padded
  static constexpr unsigned DATA_ALIGN = 4096;
  static constexpr unsigned DATA_ALIGN_MIN_LEN = 64 * 1024;

public:
  pg_shard_t from;
  spg_t pgid;
  epoch_t map_epoch = 0, min_epoch = 0;
  std::vector<PushOp> pushes;
  bool is_repair = false;

private:
  uint64_t cost = 0;

public:
  void compute_cost(CephContext *cct) {
    cost = 0;
    for (auto i = pushes.begin(); i != pushes.end(); ++i) {
      cost += i->cost(cct);
    }
  }

  int get_cost() const override {
    return cost;
  }

  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  epoch_t get_min_epoch() const override {
    return min_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  void set_cost(uint64_t c) {
    cost = c;
  }

  MOSDPGPush()
    : MOSDFastDispatchOp{MSG_OSD_PG_PUSH, HEAD_VERSION, COMPAT_VERSION}
  {}

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(pgid.pgid, p);
    decode(map_epoch, p);
    decode(pushes, p);
    decode(cost, p);
    decode(pgid.shard, p);
    decode(from, p);
    if (header.version >= 3) {
      decode(min_epoch, p);
    } else {
      min_epoch = map_epoch;
    }
    if (header.version >= 4) {
      decode(is_repair, p);
    } else {
      is_repair = false;
    }
    if (header.version > INLINE_DATA_VERSION) {
      // offset and length of each push's data in the data segment
      std::vector<std::pair<uint32_t, uint32_t>> data_extents;
      decode(data_extents, p);
      if (data_extents.size() != pushes.size()) {
        throw ceph::buffer::malformed_input(
          "MOSDPGPush: wrong number of data extents");
      }
      for (size_t i = 0; i < pushes.size(); ++i) {
        auto [off, len] = data_extents[i];
        if ((uint64_t)off + len > data.length()) {
          throw ceph::buffer::malformed_input(
            "MOSDPGPush: data extent past the data segment");
        }
        pushes[i].data.substr_of(data, off, len);
      }
    }
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    // the messenger encodes a message again after a reconnect or a change of
    // features: start from an empty data segment and set both versions.
    // Not clear_data(): it also calls clear_buffers(), which may drop the
    // pushes' data.
    data.clear();
    const bool data_segment = HAVE_FEATURE(features, SERVER_VAMPIRE);
    header.version = data_segment ? HEAD_VERSION : INLINE_DATA_VERSION;
    // older decoders would take the pushes without their data
    header.compat_version = data_segment ? HEAD_VERSION : COMPAT_VERSION;
    encode(pgid.pgid, payload);
    encode(map_epoch, payload);
    if (data_segment) {
      // the pushes go in the payload without their data
      std::vector<ceph::buffer::list> push_data(pushes.size());
      for (size_t i = 0; i < pushes.size(); ++i) {
        push_data[i].swap(pushes[i].data);
      }
      auto restore = make_scope_guard([&] {
        for (size_t i = 0; i < pushes.size(); ++i) {
          pushes[i].data.swap(push_data[i]);
        }
      });
      encode(pushes, payload, features);
    } else {
      encode(pushes, payload, features);
    }
    encode(cost, payload);
    encode(pgid.shard, payload);
    encode(from, payload);
    encode(min_epoch, payload);
    encode(is_repair, payload);
    if (data_segment) {
      std::vector<std::pair<uint32_t, uint32_t>> data_extents;
      for (const auto &push : pushes) {
        if (push.data.length() >= DATA_ALIGN_MIN_LEN) {
          data.append_zero(p2nphase<uint64_t>(data.length(), DATA_ALIGN));
        }
        data_extents.emplace_back(data.length(), push.data.length());
        data.append(push.data);
      }
      encode(data_extents, payload);
    }
  }

  std::string_view get_type_name() const override { return "MOSDPGPush"; }

  void print(std::ostream& out) const override {
    out << "MOSDPGPush(" << pgid
	<< " " << map_epoch << "/" << min_epoch
	<< " " << pushes;
    out << ")";
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
