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


#ifndef __MOSDSCRUB_H
#define __MOSDSCRUB_H

#include "msg/Message.h"

/*
 * instruct an OSD to scrub some or all pg(s)
 */

struct MOSDScrub : public Message {
  ceph_fsid fsid;
  vector<pg_t> scrub_pgs;

  MOSDScrub() {}
  MOSDScrub(ceph_fsid& f) :
    Message(MSG_OSD_SCRUB),
    fsid(f) {}
  MOSDScrub(ceph_fsid& f, vector<pg_t>& pgs) :
    Message(MSG_OSD_SCRUB),
    fsid(f), scrub_pgs(pgs) {}

  const char *get_type_name() { return "scrub"; }
  void print(ostream& out) {
    out << "scrub(";
    if (scrub_pgs.empty())
      out << "osd)";
    else
      out << scrub_pgs << ")";
  }

  void encode_payload() {
    ::encode(fsid, payload);
    ::encode(scrub_pgs, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(scrub_pgs, p);
  }
};

#endif
