// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"

class MOSDPGRestartPeering final : public Message {

  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  spg_t pgid;
  epoch_t epoch = 0;

public:
  MOSDPGRestartPeering()
    : Message(MSG_OSD_PG_RESTART_PEERING, HEAD_VERSION, COMPAT_VERSION)
  {}
  MOSDPGRestartPeering(const spg_t& pgid, epoch_t epoch)
    : Message(MSG_OSD_PG_RESTART_PEERING, HEAD_VERSION, COMPAT_VERSION),
      pgid(pgid), epoch(epoch)
  {}
  const char *get_type_name() const override { return "pg_restart_peering"; }
  void print(ostream& out) const override {
    out << get_type_name() << "(" << pgid << "@" << epoch << ")";
  }
  void encode_payload(uint64_t features) override {
    ::encode(pgid, payload);
    ::encode(epoch, payload);
  }
  void decode_payload() override {
    auto p = payload.begin();
    ::decode(pgid, p);
    ::decode(epoch, p);
  }
  epoch_t get_epoch() const {
    return epoch;
  }
  const spg_t& get_pgid() const {
    return pgid;
  }
private:
  ~MOSDPGRestartPeering() override {}
};
