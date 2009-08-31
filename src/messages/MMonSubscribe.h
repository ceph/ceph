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

#ifndef __MMONSUBSCRIBE_H
#define __MMONSUBSCRIBE_H

#include "msg/Message.h"

struct MMonSubscribe : public Message {
  struct sub_rec {
    version_t have;
    bool onetime;    // just one version, or keep sending them?
    
    void encode(bufferlist& bl) const {
      ::encode(have, bl);
      ::encode(onetime, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(have, bl);
      ::decode(onetime, bl);
    }
  };
  WRITE_CLASS_ENCODER(sub_rec)

  map<nstring, sub_rec> what;
  
  MMonSubscribe() : Message(CEPH_MSG_MON_SUBSCRIBE) {}
  
  void sub_onetime(const char *w, version_t have) {
    what[w].onetime = true;
    what[w].have = have;
  }
  void sub_persistent(const char *w, version_t have) {
    what[w].onetime = false;
    what[w].have = have;
  }

  const char *get_type_name() { return "mon_subscribe"; }
  void print(ostream& o) {
    o << "mon_subscribe(" << what << ")";
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(what, p);
  }
  void encode_payload() {
    ::encode(what, payload);
  }
};
WRITE_CLASS_ENCODER(MMonSubscribe::sub_rec)

static inline ostream& operator<<(ostream& out, const MMonSubscribe::sub_rec& r)
{
  return out << r.have << (r.onetime ? "(onetime)":"(persistent)");
}

#endif
