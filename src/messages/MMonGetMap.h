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

#ifndef CEPH_MMONGETMAP_H
#define CEPH_MMONGETMAP_H

#include "msg/Message.h"

#include "include/types.h"

class MMonGetMap final : public Message {
public:
  MMonGetMap() : Message{CEPH_MSG_MON_GET_MAP} { }
private:
  ~MMonGetMap() final {}

public:
  std::string_view get_type_name() const override { return "mon_getmap"; }
  
  void encode_payload(uint64_t features) override { }
  void decode_payload() override { }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
