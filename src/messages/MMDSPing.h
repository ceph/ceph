// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MESSAGES_MMDSPING_H
#define CEPH_MESSAGES_MMDSPING_H

#include "include/types.h"
#include "msg/Message.h"

class MMDSPing : public SafeMessage {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;
public:
  version_t seq;

protected:
  MMDSPing() : SafeMessage(MSG_MDS_PING, HEAD_VERSION, COMPAT_VERSION) {
  }
  MMDSPing(version_t seq)
    : SafeMessage(MSG_MDS_PING, HEAD_VERSION, COMPAT_VERSION), seq(seq) {
  }
  ~MMDSPing() { }

public:
  std::string_view get_type_name() const override {
    return "mdsping";
  }

  void print(ostream &out) const override {
    out << "mdsping";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(seq, payload);
  }

  void decode_payload() override {
    using ceph::decode;
    auto iter = payload.cbegin();
    decode(seq, iter);
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif // CEPH_MESSAGES_MMDSPING_H
