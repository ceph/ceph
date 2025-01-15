#pragma once

#include "PaxosServiceMessage.h"

class MOSDIgnoreSubtree : public PaxosServiceMessage {
public:
  enum {
    IGNORE = 0,
    CANCEL_IGNORE = 1,
    REPLY = 2,
  };
  __u8 op = 0;
  utime_t stamp;

  const char *get_op_name(int op) const {
    switch (op) {
    case IGNORE: return "ignore";
    case CANCEL_IGNORE: return "cancel_ignore";
    case REPLY: return "reply";
    default: return "???";
    }
  }
  MOSDIgnoreSubtree()
    : PaxosServiceMessage(MSG_OSD_IGNORE_SUBTREE, 0) {}
  MOSDIgnoreSubtree(__u8 o, utime_t s)
    : PaxosServiceMessage(MSG_OSD_IGNORE_SUBTREE, 0), op(o), stamp(s)
  {}

private:
  ~MOSDIgnoreSubtree() override {}

public:
  std::string_view get_type_name() const override { return "osd_ignore_subtree"; }
  void print(std::ostream& out) const override {
    out << "osd_ignore_subtree(" << get_op_name(op)
	<< " stamp " << stamp
	<< ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(op, payload);
    encode(stamp, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(op, p);
    decode(stamp, p);
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
