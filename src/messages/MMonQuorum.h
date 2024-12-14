// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MMONQUORUM_H
#define CEPH_MMONQUORUM_H

#include "msg/Message.h"
#include "include/encoding.h"
#include "include/ceph_features.h"
#include "msg/MessageRef.h"


class MMonQuorum : public Message
{
public:

  MMonQuorum() : Message{CEPH_MSG_MON_QUORUM} { }
  explicit MMonQuorum(entity_addrvec_t const &peer_addrs,
  epoch_t epoch, std::set<int> const &quo) : Message{CEPH_MSG_MON_QUORUM} {
    this->epoch = epoch;
    this->peer_addrs = peer_addrs;
    quorum = quo;
  }
private:
  ~MMonQuorum() {}

public:
  std::string_view get_type_name() const override { return "mon_quorum"; }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(peer_addrs, payload, features);
    encode(quorum, payload);
    encode(epoch, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(peer_addrs, p);
    decode(quorum, p);
    decode(epoch, p);
  }

  entity_addrvec_t peer_addrs;
  std::set<int> quorum;
  epoch_t epoch = 0;
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif // CEPH_MMONQUORUM_H
