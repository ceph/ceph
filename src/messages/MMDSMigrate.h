// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2026 Clyso Technologies Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MMDSMIGRATE_H
#define CEPH_MMDSMIGRATE_H

#include "include/types.h"
#include "mds/mdstypes.h"
#include "messages/MMDSOp.h"

/*
 * Between the rank that started a directory migration and the ranks holding
 * parts of the tree; see MigrateQueue.cc.  The MigrateQueue encodes data.
 */
class MMDSMigrate final : public MMDSOp {
public:
  static constexpr int OP_ITEMS   = 1;  // take these over
  static constexpr int OP_ACK     = 2;  // durable up to here, or refused
  static constexpr int OP_REPORT  = 3;  // sub-job to origin: how it is going
  static constexpr int OP_CONTROL = 4;  // origin to sub-job: what it should do

  static const char *get_opname(int o) {
    switch (o) {
    case OP_ITEMS: return "items";
    case OP_ACK: return "ack";
    case OP_REPORT: return "report";
    case OP_CONTROL: return "control";
    default: return "unknown";
    }
  }

  std::string_view get_type_name() const override { return "mds_migrate"; }

  void print(std::ostream& out) const override {
    out << "mds_migrate(" << get_opname(op) << " job " << origin << "."
        << job << ")";
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(op, payload);
    encode(origin, payload);
    encode(job, payload);
    encode(data, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(op, p);
    decode(origin, p);
    decode(job, p);
    decode(data, p);
  }

  int32_t op = 0;
  mds_rank_t origin = MDS_RANK_NONE;  // the rank the job was started on...
  uint64_t job = 0;                   // ...and its id there
  ceph::buffer::list data;

protected:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  MMDSMigrate() : MMDSOp(MSG_MDS_MIGRATE, HEAD_VERSION, COMPAT_VERSION) {}
  MMDSMigrate(int o, mds_rank_t r, uint64_t j, ceph::buffer::list&& d)
    : MMDSOp(MSG_MDS_MIGRATE, HEAD_VERSION, COMPAT_VERSION),
      op(o), origin(r), job(j), data(std::move(d)) {}
  ~MMDSMigrate() final {}

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
