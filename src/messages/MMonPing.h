// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/**
 * This is used to send pings between monitors for
 * heartbeat purposes. We include a timestamp and distinguish between
 * outgoing pings and responses to those. If you set the
 * min_message in the constructor, the message will inflate itself
 * to the specified size -- this is good for dealing with network
 * issues with jumbo frames. See http://tracker.ceph.com/issues/20087
 *
 */

#ifndef CEPH_MMONPING_H
#define CEPH_MMONPING_H

#include "common/Clock.h"

#include "msg/Message.h"
#include "mon/ConnectionTracker.h"

class MMonPing : public Message {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

 public:
  enum {
    PING = 1,
    PING_REPLY = 2,
  };
  const char *get_op_name(int op) const {
    switch (op) {
    case PING: return "ping";
    case PING_REPLY: return "ping_reply";
    default: return "???";
    }
  }

  __u8 op = 0;
  utime_t stamp;
  bufferlist tracker_bl;
  uint32_t min_message_size = 0;

  MMonPing(__u8 o, utime_t s, const bufferlist& tbl,
	   uint32_t min_message)
    : Message{MSG_MON_PING, HEAD_VERSION, COMPAT_VERSION},
      op(o), stamp(s), tracker_bl(tbl), min_message_size(min_message)
  {}
  MMonPing(__u8 o, utime_t s, const bufferlist& tbl)
    : Message{MSG_MON_PING, HEAD_VERSION, COMPAT_VERSION},
      op(o), stamp(s), tracker_bl(tbl) {}
  MMonPing()
    : Message{MSG_MON_PING, HEAD_VERSION, COMPAT_VERSION}
  {}
private:
  ~MMonPing() override {}

public:
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(op, p);
    decode(stamp, p);
    decode(tracker_bl, p);

    int payload_mid_length = p.get_off();
    uint32_t size;
    decode(size, p);
    p += size;
    min_message_size = size + payload_mid_length;
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(op, payload);
    encode(stamp, payload);
    encode(tracker_bl, payload);

    size_t s = 0;
    if (min_message_size > payload.length()) {
      s = min_message_size - payload.length();
    }
    encode((uint32_t)s, payload);
    if (s) {
      // this should be big enough for normal min_message padding sizes. since
      // we are targeting jumbo ethernet frames around 9000 bytes, 16k should
      // be more than sufficient!  the compiler will statically zero this so
      // that at runtime we are only adding a bufferptr reference to it.
      static char zeros[16384] = {};
      while (s > sizeof(zeros)) {
        payload.append(buffer::create_static(sizeof(zeros), zeros));
        s -= sizeof(zeros);
      }
      if (s) {
        payload.append(buffer::create_static(s, zeros));
      }
    }
  }

  std::string_view get_type_name() const override { return "mon_ping"; }
  void print(ostream& out) const override {
    out << "mon_ping(" << get_op_name(op)
	<< " stamp " << stamp
	<< ")";
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
