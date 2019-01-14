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
 * Foundation.  See file COPYING.
 *
 */

#pragma once

class MTimeCheck2 : public MessageInstance<MTimeCheck2> {
public:
  friend factory;

  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  enum {
    OP_PING = 1,
    OP_PONG = 2,
    OP_REPORT = 3,
  };

  int op = 0;
  version_t epoch = 0;
  version_t round = 0;

  utime_t timestamp;
  map<int, double> skews;
  map<int, double> latencies;

  MTimeCheck2() : MessageInstance(MSG_TIMECHECK2, HEAD_VERSION, COMPAT_VERSION) { }
  MTimeCheck2(int op) :
    MessageInstance(MSG_TIMECHECK2, HEAD_VERSION, COMPAT_VERSION),
    op(op)
  { }

private:
  ~MTimeCheck2() override { }

public:
  std::string_view get_type_name() const override { return "time_check2"; }
  const char *get_op_name() const {
    switch (op) {
    case OP_PING: return "ping";
    case OP_PONG: return "pong";
    case OP_REPORT: return "report";
    }
    return "???";
  }
  void print(ostream &o) const override {
    o << "time_check( " << get_op_name()
      << " e " << epoch << " r " << round;
    if (op == OP_PONG) {
      o << " ts " << timestamp;
    } else if (op == OP_REPORT) {
      o << " #skews " << skews.size()
        << " #latencies " << latencies.size();
    }
    o << " )";
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(op, p);
    decode(epoch, p);
    decode(round, p);
    decode(timestamp, p);
    decode(skews, p);
    decode(latencies, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(op, payload);
    encode(epoch, payload);
    encode(round, payload);
    encode(timestamp, payload);
    encode(skews, payload, features);
    encode(latencies, payload, features);
  }
};
