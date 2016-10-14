// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#ifndef CEPH_MMGROPEN_H_
#define CEPH_MMGROPEN_H_

#include "msg/Message.h"

class MMgrOpen : public Message
{
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

public:

  std::string daemon_name;

  void decode_payload()
  {
    bufferlist::iterator p = payload.begin();
    ::decode(daemon_name, p);
  }

  void encode_payload(uint64_t features) {
    ::encode(daemon_name, payload);
  }

  const char *get_type_name() const { return "mgropen"; }
  void print(ostream& out) const {
    out << get_type_name() << "(" << daemon_name << ")"; 
  }

  MMgrOpen()
    : Message(MSG_MGR_OPEN, HEAD_VERSION, COMPAT_VERSION)
  {}
};

#endif

