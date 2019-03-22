// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <vector>

#include <boost/smart_ptr/local_shared_ptr.hpp>
#include <boost/statechart/custom_reaction.hpp>
#include <boost/statechart/event.hpp>
#include <boost/statechart/simple_state.hpp>
#include <boost/statechart/state.hpp>
#include <boost/statechart/state_machine.hpp>
#include <boost/statechart/transition.hpp>
#include <boost/statechart/event_base.hpp>

#include "osd/PGPeeringEvent.h"

namespace recovery {

struct AdvMap : boost::statechart::event< AdvMap > {
  // TODO: use foreign_ptr<> once the osdmap cache needs to be shared across
  //       cores
  using OSDMapRef = boost::local_shared_ptr<OSDMap>;
  OSDMapRef osdmap;
  OSDMapRef last_map;
  std::vector<int> new_up, new_acting;
  int up_primary, acting_primary;
  AdvMap(OSDMapRef osdmap, OSDMapRef last_map,
	 const std::vector<int>& new_up, int up_primary,
	 const std::vector<int>& new_acting, int acting_primary):
    osdmap(osdmap),
    last_map(last_map),
    new_up(new_up),
    new_acting(new_acting),
    up_primary(up_primary),
    acting_primary(acting_primary) {}
  void print(std::ostream *out) const {
    *out << "AdvMap";
  }
};

struct ActMap : boost::statechart::event< ActMap > {
  ActMap() : boost::statechart::event< ActMap >() {}
  void print(std::ostream *out) const {
    *out << "ActMap";
  }
};

struct Activate : boost::statechart::event< Activate > {
  epoch_t activation_epoch;
  explicit Activate(epoch_t q) : boost::statechart::event< Activate >(),
				 activation_epoch(q) {}
  void print(std::ostream *out) const {
    *out << "Activate from " << activation_epoch;
  }
};

struct Initialize : boost::statechart::event<Initialize> {};

}
