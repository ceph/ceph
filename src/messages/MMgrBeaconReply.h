// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MESSAGES_MMGR_BEACON_REPLY_H
#define CEPH_MESSAGES_MMGR_BEACON_REPLY_H

#include "include/types.h"

#include "msg/Message.h"

class MMgrBeaconReply : public SafeMessage {
public:
  MMgrBeaconReply()
    : SafeMessage(MSG_MGR_BEACON_REPLY, HEAD_VERSION, COMPAT_VERSION) {
  }
  MMgrBeaconReply(version_t seq_ack)
    : SafeMessage(MSG_MGR_BEACON_REPLY, HEAD_VERSION, COMPAT_VERSION),
      seq_ack(seq_ack) {
  }

  version_t get_seq_ack() const {
    return seq_ack;
  }

  std::string_view get_type_name() const override { return "mgr_beacon_reply"; }
  void print(std::ostream& o) const override {
    o << "mgr_beacon_reply (seq_ack #" << seq_ack << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(seq_ack, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto iter = payload.cbegin();
    decode(seq_ack, iter);
  }

private:
  ~MMgrBeaconReply() override {}

  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;
  version_t seq_ack = 0;

  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif // CEPH_MESSAGES_MMGR_BEACON_REPLY_H
