// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"
#include "osd/osd_types.h"
#include "osd/PGPeeringEvent.h"

class MOSDPeeringOp : public MessageSubType<MOSDPeeringOp> {
public:

template<typename... Args>
  MOSDPeeringOp(Args&&... args) : MessageSubType(std::forward<Args>(args)...) {}

  void print(std::ostream& out) const override final {
    out << get_type_name() << "("
	<< get_spg() << " ";
    inner_print(out);
    out << " e" << get_map_epoch() << "/" << get_min_epoch() << ")";
  }

  virtual spg_t get_spg() const = 0;
  virtual epoch_t get_map_epoch() const = 0;
  virtual epoch_t get_min_epoch() const = 0;
  virtual PGPeeringEvent *get_event() = 0;
  virtual void inner_print(std::ostream& out) const = 0;
};
