// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


/**
 * This is used to send pings between daemons (so far, the OSDs) for
 * heartbeat purposes. We include a timestamp and distinguish between
 * outgoing pings and responses to those. If you set the
 * min_message in the constructor, the message will inflate itself
 * to the specified size -- this is good for dealing with network
 * issues with jumbo frames. See http://tracker.ceph.com/issues/20087
 *
 */

#ifndef CEPH_MOSDPING_H
#define CEPH_MOSDPING_H

#include "common/Clock.h"

#include "msg/Message.h"
#include "osd/osd_types.h"


class MOSDPing : public Message {
private:
  static constexpr int HEAD_VERSION = 5;
  static constexpr int COMPAT_VERSION = 4;

 public:
  enum {
    HEARTBEAT = 0,
    START_HEARTBEAT = 1,
    YOU_DIED = 2,
    STOP_HEARTBEAT = 3,
    PING = 4,
    PING_REPLY = 5,
  };
  const char *get_op_name(int op) const {
    switch (op) {
    case HEARTBEAT: return "heartbeat";
    case START_HEARTBEAT: return "start_heartbeat";
    case STOP_HEARTBEAT: return "stop_heartbeat";
    case YOU_DIED: return "you_died";
    case PING: return "ping";
    case PING_REPLY: return "ping_reply";
    default: return "???";
    }
  }

  uuid_d fsid;
  epoch_t map_epoch = 0;
  __u8 op = 0;
  utime_t ping_stamp;               ///< when the PING was sent
  ceph::signedspan mono_ping_stamp; ///< relative to sender's clock
  ceph::signedspan mono_send_stamp; ///< replier's send stamp
  std::optional<ceph::time_detail::signedspan> delta_ub;  ///< ping sender
  epoch_t up_from = 0;

  uint32_t min_message_size = 0;

  MOSDPing(const uuid_d& f, epoch_t e, __u8 o,
	   utime_t s,
	   ceph::signedspan ms,
	   ceph::signedspan mss,
	   epoch_t upf,
	   uint32_t min_message,
	   std::optional<ceph::time_detail::signedspan> delta_ub = {})
    : Message{MSG_OSD_PING, HEAD_VERSION, COMPAT_VERSION},
      fsid(f), map_epoch(e), op(o),
      ping_stamp(s),
      mono_ping_stamp(ms),
      mono_send_stamp(mss),
      delta_ub(delta_ub),
      up_from(upf),
      min_message_size(min_message)
  { }
  MOSDPing()
    : Message{MSG_OSD_PING, HEAD_VERSION, COMPAT_VERSION}
  {}
private:
  ~MOSDPing() override {}

public:
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(fsid, p);
    decode(map_epoch, p);
    decode(op, p);
    decode(ping_stamp, p);

    int payload_mid_length = p.get_off();
    uint32_t size;
    decode(size, p);

    if (header.version >= 5) {
      decode(up_from, p);
      decode(mono_ping_stamp, p);
      decode(mono_send_stamp, p);
      decode(delta_ub, p);
    }

    p.advance(size);
    min_message_size = size + payload_mid_length;
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(fsid, payload);
    encode(map_epoch, payload);
    encode(op, payload);
    encode(ping_stamp, payload);

    size_t s = 0;
    if (min_message_size > payload.length()) {
      s = min_message_size - payload.length();
    }
    encode((uint32_t)s, payload);

    encode(up_from, payload);
    encode(mono_ping_stamp, payload);
    encode(mono_send_stamp, payload);
    encode(delta_ub, payload);

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

  std::string_view get_type_name() const override { return "osd_ping"; }
  void print(ostream& out) const override {
    out << "osd_ping(" << get_op_name(op)
	<< " e" << map_epoch
	<< " up_from " << up_from
	<< " ping_stamp " << ping_stamp << "/" << mono_ping_stamp
	<< " send_stamp " << mono_send_stamp;
    if (delta_ub) {
      out << " delta_ub " << *delta_ub;
    }
    out << ")";
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
