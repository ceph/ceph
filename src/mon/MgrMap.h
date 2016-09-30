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

#include <sstream>

#include "msg/msg_types.h"
#include "common/Formatter.h"
#include "include/encoding.h"

class StandbyInfo
{
public:
  uint64_t gid;
  std::string name;

  StandbyInfo(uint64_t gid_, const std::string &name_)
    : gid(gid_), name(name_)
  {}

  StandbyInfo()
    : gid(0)
  {}

  void encode(bufferlist& bl) const
  {
    ENCODE_START(1, 1, bl);
    ::encode(gid, bl);
    ::encode(name, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& p)
  {
    DECODE_START(1, p);
    ::decode(gid, p);
    ::decode(name, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(StandbyInfo)

class MgrMap
{
public:
  epoch_t epoch;

  // global_id of the ceph-mgr instance selected as a leader
  uint64_t active_gid;
  // server address reported by the leader once it is active
  entity_addr_t active_addr;
  // whether the nominated leader is active (i.e. has initialized its server)
  bool available;
  // the name (foo in mgr.<foo>) of the active daemon
  std::string active_name;

  std::map<uint64_t, StandbyInfo> standbys;

  epoch_t get_epoch() const { return epoch; }
  entity_addr_t get_active_addr() const { return active_addr; }
  uint64_t get_active_gid() const { return active_gid; }
  bool get_available() const { return available; }
  const std::string &get_active_name() const { return active_name; }

  void encode(bufferlist& bl, uint64_t features) const
  {
    ENCODE_START(1, 1, bl);
    ::encode(epoch, bl);
    ::encode(active_addr, bl, features);
    ::encode(active_gid, bl);
    ::encode(available, bl);
    ::encode(active_name, bl);
    ::encode(standbys, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& p)
  {
    DECODE_START(1, p);
    ::decode(epoch, p);
    ::decode(active_addr, p);
    ::decode(active_gid, p);
    ::decode(available, p);
    ::decode(active_name, p);
    ::decode(standbys, p);
    DECODE_FINISH(p);
  }

  void print_summary(Formatter *f, std::ostream *ss) const
  {
    // One or the other, not both
    assert((ss != nullptr) != (f != nullptr));

    if (f) {
      f->dump_int("active_gid", get_active_gid());
      f->dump_string("active_name", get_active_name());
    } else {
      if (get_active_gid() != 0) {
        *ss << "active: " << get_active_name() << " ";
      } else {
        *ss << "no daemons active ";
      }
    }


    if (f) {
      f->open_array_section("standbys");
      for (const auto &i : standbys) {
        f->open_object_section("standby");
        f->dump_int("gid", i.second.gid);
        f->dump_string("name", i.second.name);
        f->close_section();
      }
      f->close_section();
    } else {
      if (standbys.size()) {
        *ss << "standbys: ";
        bool first = true;
        for (const auto &i : standbys) {
          if (!first) {
            *ss << ", ";
          }
          *ss << i.second.name;
          first = false;
        }
      }
    }
  }

  MgrMap()
    : epoch(0), available(false)
  {}
};

WRITE_CLASS_ENCODER_FEATURES(MgrMap)

#endif

