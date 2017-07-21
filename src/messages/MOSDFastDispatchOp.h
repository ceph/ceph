// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MOSDFASTDISPATCHOP_H
#define CEPH_MOSDFASTDISPATCHOP_H

#include "msg/Message.h"
#include "osd/osd_types.h"

class MOSDFastDispatchOp : public Message {
public:
  virtual epoch_t get_map_epoch() const = 0;
  virtual epoch_t get_min_epoch() const {
    return get_map_epoch();
  }
  virtual spg_t get_spg() const = 0;

  MOSDFastDispatchOp(int t, int version, int compat_version)
    : Message(t, version, compat_version) {}
};

#endif
