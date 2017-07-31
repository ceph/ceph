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


#ifndef CEPH_MMGRCONFIGURE_H_
#define CEPH_MMGRCONFIGURE_H_

#include "msg/Message.h"

/**
 * This message is sent from ceph-mgr to MgrClient, instructing it
 * it about what data to send back to ceph-mgr at what frequency.
 */
class MMgrConfigure : public Message
{
  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;

public:
  uint32_t stats_period;
  
  // Default 0 means if unspecified will include all stats
  uint32_t stats_threshold = 0;

  void decode_payload() override
  {
    bufferlist::iterator p = payload.begin();
    ::decode(stats_period, p);
    if (header.version >= 2) {
      ::decode(stats_threshold, p);
    }
  }

  void encode_payload(uint64_t features) override {
    ::encode(stats_period, payload);
    ::encode(stats_threshold, payload);
  }

  const char *get_type_name() const override { return "mgrconfigure"; }
  void print(ostream& out) const override {
    out << get_type_name() << "(period=" << stats_period
                           << ", threshold=" << stats_threshold << ")";
  }

  MMgrConfigure()
    : Message(MSG_MGR_CONFIGURE, HEAD_VERSION, COMPAT_VERSION)
  {}
};

#endif

