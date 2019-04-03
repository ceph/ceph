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

#ifndef CEPH_MMONSUBSCRIBE_H
#define CEPH_MMONSUBSCRIBE_H

#include "msg/Message.h"
#include "include/ceph_features.h"

/*
 * compatibility with old crap
 */
struct ceph_mon_subscribe_item_old {
	__le64 unused;
	__le64 have;
	__u8 onetime;
} __attribute__ ((packed));
WRITE_RAW_ENCODER(ceph_mon_subscribe_item_old)


class MMonSubscribe : public MessageInstance<MMonSubscribe> {
public:
  friend factory;

  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 1;

  std::string hostname;
  std::map<std::string, ceph_mon_subscribe_item> what;

  MMonSubscribe() : MessageInstance(CEPH_MSG_MON_SUBSCRIBE, HEAD_VERSION, COMPAT_VERSION) { }
private:
  ~MMonSubscribe() override {}

public:
  void sub_want(const char *w, version_t start, unsigned flags) {
    what[w].start = start;
    what[w].flags = flags;
  }

  std::string_view get_type_name() const override { return "mon_subscribe"; }
  void print(std::ostream& o) const override {
    o << "mon_subscribe(" << what << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    if (header.version < 2) {
      std::map<std::string, ceph_mon_subscribe_item_old> oldwhat;
      decode(oldwhat, p);
      what.clear();
      for (auto q = oldwhat.begin(); q != oldwhat.end(); q++) {
	if (q->second.have)
	  what[q->first].start = q->second.have + 1;
	else
	  what[q->first].start = 0;
	what[q->first].flags = 0;
	if (q->second.onetime)
	  what[q->first].flags |= CEPH_SUBSCRIBE_ONETIME;
      }
      return;
    }
    decode(what, p);
    if (header.version >= 3) {
      decode(hostname, p);
    }
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    if ((features & CEPH_FEATURE_SUBSCRIBE2) == 0) {
      header.version = 0;
      std::map<std::string, ceph_mon_subscribe_item_old> oldwhat;
      for (auto q = what.begin(); q != what.end(); q++) {
	if (q->second.start)
	  // warning: start=1 -> have=0, which was ambiguous
	  oldwhat[q->first].have = q->second.start - 1;
	else
	  oldwhat[q->first].have = 0;
	oldwhat[q->first].onetime = q->second.flags & CEPH_SUBSCRIBE_ONETIME;
      }
      encode(oldwhat, payload);
      return;
    }
    header.version = HEAD_VERSION;
    encode(what, payload);
    encode(hostname, payload);
  }
};

#endif
