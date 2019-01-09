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

class MOSDScrub : public MessageInstance<MOSDScrub> {
public:
  friend factory;

  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 2;

  uuid_d fsid;
  vector<pg_t> scrub_pgs;
  bool repair = false;
  bool deep = false;

  MOSDScrub() : MessageInstance(MSG_OSD_SCRUB, HEAD_VERSION, COMPAT_VERSION) {}
  MOSDScrub(const uuid_d& f, bool r, bool d) :
    MessageInstance(MSG_OSD_SCRUB, HEAD_VERSION, COMPAT_VERSION),
    fsid(f), repair(r), deep(d) {}
  MOSDScrub(const uuid_d& f, vector<pg_t>& pgs, bool r, bool d) :
    MessageInstance(MSG_OSD_SCRUB, HEAD_VERSION, COMPAT_VERSION),
    fsid(f), scrub_pgs(pgs), repair(r), deep(d) {}
private:
  ~MOSDScrub() override {}

public:
  std::string_view get_type_name() const override { return "scrub"; }
  void print(ostream& out) const override {
    out << "scrub(";
    if (scrub_pgs.empty())
      out << "osd";
    else
      out << scrub_pgs;
    if (repair)
      out << " repair";
    if (deep)
      out << " deep";
    out << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(fsid, payload);
    encode(scrub_pgs, payload);
    encode(repair, payload);
    encode(deep, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(fsid, p);
    decode(scrub_pgs, p);
    decode(repair, p);
    decode(deep, p);
  }
};

#endif
