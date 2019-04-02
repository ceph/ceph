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


#ifndef CEPH_MMGRMAP_H
#define CEPH_MMGRMAP_H

#include "msg/Message.h"
#include "mon/MgrMap.h"

class MMgrMap : public MessageInstance<MMgrMap> {
public:
  friend factory;

protected:
  MgrMap map;

public:
  const MgrMap & get_map() {return map;}

  MMgrMap() : 
    MessageInstance(MSG_MGR_MAP) {}
  MMgrMap(const MgrMap &map_) :
    MessageInstance(MSG_MGR_MAP), map(map_)
  {
  }

private:
  ~MMgrMap() override {}

public:
  std::string_view get_type_name() const override { return "mgrmap"; }
  void print(std::ostream& out) const override {
    out << get_type_name() << "(e " << map.epoch << ")";
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(map, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(map, payload, features);
  }
};

#endif
