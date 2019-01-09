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


#ifndef CEPH_MWATCHNOTIFY_H
#define CEPH_MWATCHNOTIFY_H

#include "msg/Message.h"


class MWatchNotify : public MessageInstance<MWatchNotify> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 1;

 public:
  uint64_t cookie;     ///< client unique id for this watch or notify
  uint64_t ver;        ///< unused
  uint64_t notify_id;  ///< osd unique id for a notify notification
  uint8_t opcode;      ///< CEPH_WATCH_EVENT_*
  bufferlist bl;       ///< notify payload (osd->client)
  errorcode32_t return_code; ///< notify result (osd->client)
  uint64_t notifier_gid; ///< who sent the notify

  MWatchNotify()
    : MessageInstance(CEPH_MSG_WATCH_NOTIFY, HEAD_VERSION, COMPAT_VERSION) { }
  MWatchNotify(uint64_t c, uint64_t v, uint64_t i, uint8_t o, bufferlist b)
    : MessageInstance(CEPH_MSG_WATCH_NOTIFY, HEAD_VERSION, COMPAT_VERSION),
      cookie(c),
      ver(v),
      notify_id(i),
      opcode(o),
      bl(b),
      return_code(0),
      notifier_gid(0) { }
private:
  ~MWatchNotify() override {}

public:
  void decode_payload() override {
    uint8_t msg_ver;
    auto p = payload.cbegin();
    decode(msg_ver, p);
    decode(opcode, p);
    decode(cookie, p);
    decode(ver, p);
    decode(notify_id, p);
    if (msg_ver >= 1)
      decode(bl, p);
    if (header.version >= 2)
      decode(return_code, p);
    else
      return_code = 0;
    if (header.version >= 3)
      decode(notifier_gid, p);
    else
      notifier_gid = 0;
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    uint8_t msg_ver = 1;
    encode(msg_ver, payload);
    encode(opcode, payload);
    encode(cookie, payload);
    encode(ver, payload);
    encode(notify_id, payload);
    encode(bl, payload);
    encode(return_code, payload);
    encode(notifier_gid, payload);
  }

  std::string_view get_type_name() const override { return "watch-notify"; }
  void print(ostream& out) const override {
    out << "watch-notify("
	<< ceph_watch_event_name(opcode) << " (" << (int)opcode << ")"
	<< " cookie " << cookie
	<< " notify " << notify_id
	<< " ret " << return_code
	<< ")";
  }
};

#endif
