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


struct MMonSubscribe : public Message {

  static const int HEAD_VERSION = 2;

  map<string, ceph_mon_subscribe_item> what;
  
  MMonSubscribe() : Message(CEPH_MSG_MON_SUBSCRIBE, HEAD_VERSION) { }
private:
  ~MMonSubscribe() {}

public:  
  void sub_want(const char *w, version_t start, unsigned flags) {
    what[w].start = start;
    what[w].flags = flags;
  }

  const char *get_type_name() const { return "mon_subscribe"; }
  void print(ostream& o) const {
    o << "mon_subscribe(" << what << ")";
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    if (header.version < 2) {
      map<string, ceph_mon_subscribe_item_old> oldwhat;
      ::decode(oldwhat, p);
      what.clear();
      for (map<string, ceph_mon_subscribe_item_old>::iterator q = oldwhat.begin();
	   q != oldwhat.end();
	   q++) {
	if (q->second.have)
	  what[q->first].start = q->second.have + 1;
	else
	  what[q->first].start = 0;
	what[q->first].flags = 0;
	if (q->second.onetime)
	  what[q->first].flags |= CEPH_SUBSCRIBE_ONETIME;
      }
    } else {
      ::decode(what, p);
    }
  }
  void encode_payload(uint64_t features) {
    if (features & CEPH_FEATURE_SUBSCRIBE2) {
      ::encode(what, payload);
      header.version = HEAD_VERSION;
    } else {
      header.version = 0;
      map<string, ceph_mon_subscribe_item_old> oldwhat;
      for (map<string, ceph_mon_subscribe_item>::iterator q = what.begin();
	   q != what.end();
	   q++) {
	if (q->second.start)
	  // warning: start=1 -> have=0, which was ambiguous
	  oldwhat[q->first].have = q->second.start - 1;
	else
	  oldwhat[q->first].have = 0;
	oldwhat[q->first].onetime = q->second.flags & CEPH_SUBSCRIBE_ONETIME;
      }
      ::encode(oldwhat, payload);
    }
  }
};

#endif
