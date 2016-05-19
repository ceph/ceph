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

#ifndef MGR_MAP_H_
#define MGR_MAP_H_

#include "msg/msg_types.h"
#include "include/encoding.h"

class MgrMap
{
public:
  uint64_t active_gid;
  entity_addr_t active_addr;
  epoch_t epoch;

  epoch_t get_epoch() const { return epoch; }
  entity_addr_t get_active_addr() const { return active_addr; }
  uint64_t get_active_gid() const { return active_gid; }

  void encode(bufferlist& bl, uint64_t features) const
  {
    ENCODE_START(1, 1, bl);
    ::encode(epoch, bl);
    ::encode(active_addr, bl, features);
    ::encode(active_gid, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& p)
  {
    DECODE_START(1, p);
    ::decode(epoch, p);
    ::decode(active_addr, p);
    ::decode(active_gid, p);
    DECODE_FINISH(p);
  }

  MgrMap()
    : epoch(0)
  {}
};

WRITE_CLASS_ENCODER_FEATURES(MgrMap)

#endif

