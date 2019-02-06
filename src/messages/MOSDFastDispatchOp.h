// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MOSDFASTDISPATCHOP_H
#define CEPH_MOSDFASTDISPATCHOP_H

#include "msg/Message.h"
#include "osd/osd_types.h"

class MOSDFastDispatchOp : public MessageSubType<MOSDFastDispatchOp> {
public:

template<typename... Args>
  MOSDFastDispatchOp(Args&&... args) : MessageSubType<MOSDFastDispatchOp>(std::forward<Args>(args)...) {}

  virtual epoch_t get_map_epoch() const = 0;
  virtual epoch_t get_min_epoch() const {
    return get_map_epoch();
  }
  virtual spg_t get_spg() const = 0;
};

#endif
