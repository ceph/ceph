// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_MMONSHUTDOWN_H
#define CEPH_MMONSHUTDOWN_H

#include "msg/Message.h"

class MMonShutdown final : public Message {
public:
  std::string name;
  MMonShutdown() : Message{MSG_MON_SHUTDOWN} {}
  MMonShutdown(std::string n) : Message{MSG_MON_SHUTDOWN},
  name(n) {}
private:
  ~MMonShutdown() final {}

public:
  std::string_view get_type_name() const override { return "mon_shutdown"; }
  void print(std::ostream& o) const override {
    o << "mon_shutdown: mon." << name;
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(name, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(name, payload);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
