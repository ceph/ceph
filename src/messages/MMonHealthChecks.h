// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MMON_HEALTH_CHECKS_H
#define CEPH_MMON_HEALTH_CHECKS_H

#include "messages/PaxosServiceMessage.h"
#include "mon/health_check.h"

class MMonHealthChecks final : public PaxosServiceMessage {
public:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  health_check_map_t health_checks;

  MMonHealthChecks()
    : PaxosServiceMessage{MSG_MON_HEALTH_CHECKS, HEAD_VERSION, COMPAT_VERSION} {
  }
  MMonHealthChecks(health_check_map_t& m)
    : PaxosServiceMessage{MSG_MON_HEALTH_CHECKS, HEAD_VERSION, COMPAT_VERSION},
      health_checks(m)
  {}

private:
  ~MMonHealthChecks() final { }

public:
  std::string_view get_type_name() const override { return "mon_health_checks"; }
  void print(std::ostream &o) const override {
    o << "mon_health_checks(" << health_checks.checks.size() << " checks)";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(health_checks, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(health_checks, payload);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
