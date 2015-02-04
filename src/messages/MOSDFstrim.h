// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *                         Xinze Chi <xmdxcxz@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_MOSDFSTRIM_H
#define CEPH_MOSDFSTRIM_H

#include "msg/Message.h"

/*
 * instruct an OSD to fstrim filestore
 */

struct MOSDFstrim : public Message {

  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

  uuid_d fsid;

  MOSDFstrim() : Message(MSG_OSD_FSTRIM, HEAD_VERSION, COMPAT_VERSION) {}
  MOSDFstrim(const uuid_d& f) :
    Message(MSG_OSD_FSTRIM, HEAD_VERSION, COMPAT_VERSION),
    fsid(f) {}
private:
  ~MOSDFstrim() {}

public:
  const char *get_type_name() const { return "fstrim"; }
  void print(ostream& out) const {
    out << "fstrim(osd)";
  }

  void encode_payload(uint64_t features) {
    ::encode(fsid, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
  }
};

#endif
