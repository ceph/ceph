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


#ifndef CEPH_MOSDSCRUB_H
#define CEPH_MOSDSCRUB_H

#include "msg/Message.h"

/*
 * instruct an OSD to scrub some or all pg(s)
 */

struct MOSDScrub : public Message {
  ceph_fsid_t fsid;
  vector<pg_t> scrub_pgs;
  bool repair;

  MOSDScrub() {}
  MOSDScrub(ceph_fsid_t& f, bool r) :
    Message(MSG_OSD_SCRUB),
    fsid(f), repair(r) {}
  MOSDScrub(ceph_fsid_t& f, vector<pg_t>& pgs, bool r) :
    Message(MSG_OSD_SCRUB),
    fsid(f), scrub_pgs(pgs), repair(r) {}
private:
  ~MOSDScrub() {}

public:
  const char *get_type_name() { return "scrub"; }
  void print(ostream& out) {
    out << "scrub(";
    if (scrub_pgs.empty())
      out << "osd";
    else
      out << scrub_pgs;
    if (repair)
      out << " repair";
    out << ")";
  }

  void encode_payload() {
    ::encode(fsid, payload);
    ::encode(scrub_pgs, payload);
    ::encode(repair, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(scrub_pgs, p);
    ::decode(repair, p);
  }
};

#endif
